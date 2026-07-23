(ns dbos.client
  "Out-of-process workflow enqueuing via `DBOSClient`.

  Stateless: `create-client` builds a client wired with the transit
  serializer; the consumer owns the state cell and lifecycle (`.close` on
  shutdown). Unlike the core DBOS instance there is no launch/shutdown
  wrapper — a client holds no running workflows, so it has no drain
  obligation."
  (:require
   [dbos.core :as core]
   [dbos.serializer :as serializer]
   [taoensso.trove :as trove])
  (:import
   (dev.dbos.transact DBOSClient)
   (dev.dbos.transact.json DBOSSerializer)
   (javax.sql DataSource)))

(defn create-client
  "Build a DBOSClient wired with the transit serializer (so callers pass raw
  Clojure input). The caller owns lifecycle — call `.close` on shutdown.

  - :datasource   any `javax.sql.DataSource` (e.g. a HikariCP pool), OR
  - :database-url + :db-user + :db-password
  - :schema       optional DBOS schema for the system tables
  - :serializer   optional DBOSSerializer (defaults to the transit serializer)"
  ^DBOSClient
  [{:keys [datasource database-url db-user db-password schema serializer]}]
  (let [^DBOSSerializer serializer (or serializer (serializer/transit-serializer))]
    (cond
      datasource
      (DBOSClient. ^DataSource datasource ^String schema serializer)

      database-url
      (DBOSClient. ^String database-url ^String db-user ^String db-password
                   ^String schema serializer)

      :else
      (throw (ex-info "create-client needs :datasource or :database-url"
                      {:error/type :dbos.client/missing-datasource})))))

(defn enqueue-workflow!
  "Enqueue a workflow for async execution by a DBOS worker listening on the
  target queue (fire-and-forget; progress tracked durably by DBOS). The
  (workflowName, className) pair is derived from `wf-key` via
  `workflow-identity`; there's no client-side registry, so a typo'd keyword
  surfaces as a durable NOT_FOUND at the worker.

  - `client`        a DBOSClient from `create-client`
  - `wf-key`        keyword the workflow is registered under
  - `id-or-opts`    a workflow-id string, an options map (see
                    `dbos.core/->workflow-opts`), or a pre-built
                    DBOSClient$EnqueueOptions. A queue is REQUIRED for the
                    string/map forms — set :workflow/queue (a name string or
                    Queue instance); a pre-built options object encodes its own
                    queue, so the check is skipped for it
  - `workflow-data` the single serializable workflow argument

  The workflow id is available inside the body via `(dbos.core/workflow-id)`
  and on the returned handle — it is NOT injected into `workflow-data`.

  (enqueue-workflow! client :my/wf
                     {:workflow/id \"wf-1\" :workflow/queue \"my-queue\"}
                     {:some \"input\"})

  Returns a deref-able `WorkflowHandle`: `@handle` blocks for the result."
  [^DBOSClient client wf-key id-or-opts workflow-data]
  (let [{:keys [wf-name class-name]} (core/workflow-identity wf-key)
        opts (core/->workflow-opts id-or-opts)
        built (::core/built opts)]
    (if built
      (do
        (trove/log! {:id :dbos.client.workflow/enqueue
                     :data {:workflow/key wf-key
                            :workflow/class class-name
                            :workflow/built true}})
        (core/add-derefable
         (.enqueueWorkflow client
                           (core/->enqueue-options built)
                           (object-array [workflow-data]))))
      (let [{:keys [workflow-id queue]} opts]
        (when-not queue
          (throw (ex-info "enqueue-workflow! requires a queue - set :workflow/queue"
                          {:error/type :dbos.client/missing-queue
                           :workflow/key wf-key
                           :workflow/id workflow-id})))
        (trove/log! {:id :dbos.client.workflow/enqueue
                     :data {:workflow/key wf-key
                            :workflow/class class-name
                            :workflow/id workflow-id
                            :queue queue}})
        (core/add-derefable
         (.enqueueWorkflow client
                           (core/->enqueue-options
                            (-> opts
                                (assoc :workflow-name wf-name
                                       :class-name class-name)
                                (update :app-version
                                        #(core/resolve-app-version client %))))
                           (object-array [workflow-data])))))))

(defn retrieve-workflow
  "Retrieve a handle to an existing workflow by `workflow-id` via the client,
  returning a deref-able `WorkflowHandle` (`@handle` blocks for the result)."
  [^DBOSClient client workflow-id]
  (core/add-derefable (.retrieveWorkflow client ^String workflow-id)))
