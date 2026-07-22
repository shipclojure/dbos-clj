(ns dbos.example.system
  "A minimal Integrant system wiring the dbos-clj library the way a real
  consumer (digital-worker-poc) does:

  - `:example/datasource` — a HikariCP pool DBOS persists to;
  - `:dbos/instance`      — builds and launches DBOS over that datasource,
                            registering the `#ig/refset :dbos/workflow`
                            definitions and the example queue, and returning
                            the RAW DBOS instance (the same object a workflow
                            body receives, and what `dbos.core/start-workflow!`
                            takes);
  - Telemere is wired as the logging backend through Trove, and each step's
    {:workflow/step ..} is merged into Telemere's ambient context.

  Boot with `start!`, tear down with `stop!`. The static structure lives in
  resources/dbos_example/system.edn; DB creds are overlaid from the
  environment so the same config runs locally and in CI."
  (:require
   [clojure.java.io :as io]
   [dbos.core :as dbos]
   [dbos.example.serializer :as serializer]
   ;; loaded for its :dbos/workflow derives + ig/init-key methods
   [dbos.example.workflows]
   [integrant.core :as ig]
   [taoensso.telemere :as t]
   [taoensso.trove :as trove]
   [taoensso.trove.telemere :as trove-telemere])
  (:import
   (com.zaxxer.hikari HikariConfig HikariDataSource)
   (dev.dbos.transact.workflow Queue)))

(def queue-name "dbos-example-queue")

;; -- Datasource component ----------------------------------------------------

(defn- hikari-datasource
  ^HikariDataSource [{:keys [jdbc-url username password max-pool-size]}]
  (HikariDataSource.
   (doto (HikariConfig.)
     (.setJdbcUrl jdbc-url)
     (.setUsername username)
     (.setPassword password)
     (.setMaximumPoolSize (int (or max-pool-size 4))))))

(defmethod ig/init-key :example/datasource [_ config]
  (hikari-datasource config))

(defmethod ig/halt-key! :example/datasource [_ ^HikariDataSource ds]
  (.close ds))

;; -- Logging: Trove -> Telemere ----------------------------------------------

(defn configure-logging!
  "Point the library's Trove facade at Telemere and merge each step's
  {:workflow/step ..} into Telemere's ambient context, so library step logs
  and anything logged inside a step body land in Telemere with the step name
  attached. Called once at startup, before the instance launches."
  []
  (trove/set-log-fn! (trove-telemere/get-log-fn))
  (dbos/set-step-context-fn! (fn [ctx thunk] (t/with-ctx+ ctx (thunk)))))

;; -- DBOS instance component -------------------------------------------------

(defmethod ig/init-key :dbos/instance
  [_ {:keys [datasource app-name workflows]}]
  (configure-logging!)
  (let [instance (dbos/create
                  {:config {:datasource datasource
                            :app-name app-name
                            :serializer (serializer/transit-serializer)}
                   :queues [(-> (Queue. queue-name)
                                (.withWorkerConcurrency (int 4)))]
                   :workflows workflows})]
    (dbos/launch! instance)
    (t/log! {:id :dbos.example/started :data {:app-name app-name}}
            "DBOS example launched")
    instance))

(defmethod ig/halt-key! :dbos/instance [_ instance]
  (when instance
    (dbos/shutdown! instance)
    (t/log! {:id :dbos.example/stopped} "DBOS example shut down")))

;; -- Config + lifecycle ------------------------------------------------------

(defn- env-datasource-overrides
  "DB connection overrides from env vars, applied only when set so
  system.edn's defaults stand in otherwise."
  []
  (cond-> {}
    (System/getenv "DBOS_TEST_DATABASE_URL")
    (assoc :jdbc-url (System/getenv "DBOS_TEST_DATABASE_URL"))
    (System/getenv "DBOS_TEST_DB_USER")
    (assoc :username (System/getenv "DBOS_TEST_DB_USER"))
    (System/getenv "DBOS_TEST_DB_PASSWORD")
    (assoc :password (System/getenv "DBOS_TEST_DB_PASSWORD"))))

(defn config
  "The Integrant config: static structure from
  resources/dbos_example/system.edn (workflows + wiring, including
  `#ig/refset :dbos/workflow`), with DB creds overlaid from the environment."
  []
  (-> (ig/read-string (slurp (io/resource "dbos_example/system.edn")))
      (update :example/datasource merge (env-datasource-overrides))))

(defn start!
  "Build and launch the example system, returning the Integrant system map.
  `(:dbos/instance system)` is the raw DBOS instance."
  []
  (ig/init (config)))

(defn stop!
  "Halt an Integrant system map returned by `start!`."
  [system]
  (ig/halt! system))
