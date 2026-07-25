(ns dbos.test-system
  "Shared fixture for the live-DBOS tests: boots the minimalistic Integrant
  example (`dbos.example.system`, under example/) against a throwaway Postgres,
  so tests exercise the library exactly as a real consumer wires it.

  Requires a reachable Postgres. Configure via env vars (defaults in parens):
    DBOS_TEST_DATABASE_URL (jdbc:postgresql://localhost:5432/dbos_clj_test)
    DBOS_TEST_DB_USER      (postgres)
    DBOS_TEST_DB_PASSWORD  (postgres)

  `bb test` loads these from .env; DBOS creates its own system schema."
  (:require
   [dbos.client :as client]
   [dbos.example.serializer :as serializer]
   [dbos.example.system :as system]))

(def queue-name system/queue-name)

(def ^:dynamic *instance* nil)
(def ^:dynamic *client* nil)

(defn- env [k default] (or (System/getenv k) default))

(defn with-example-system
  "A `:once` fixture binding `*instance*` and `*client*` for the test run."
  [f]
  (let [sys (system/start!)
        the-client (client/create-client
                    {:database-url (env "DBOS_TEST_DATABASE_URL"
                                        "jdbc:postgresql://localhost:5432/dbos_clj_test")
                     :db-user (env "DBOS_TEST_DB_USER" "postgres")
                     :db-password (env "DBOS_TEST_DB_PASSWORD" "postgres")
                     ;; the client must share the instance's serializer so the
                     ;; two agree on the wire format (java.time handlers, etc.)
                     :serializer (serializer/transit-serializer)})]
    (try
      (binding [*instance* (:dbos/instance sys)
                *client* the-client]
        (f))
      (finally
        (.close the-client)
        (system/stop! sys)))))
