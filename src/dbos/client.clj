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
  - `id-or-opts`    a workflow-id string, or an options map (see
                    `dbos.core/->workflow-opts`). A queue is REQUIRED — set
                    :workflow/queue (a name string or Queue instance)
  - `workflow-data` input map; `:workflow/id` is merged in automatically

  (enqueue-workflow! client :my/wf
                     {:workflow/id \"wf-1\" :workflow/queue \"my-queue\"}
                     {:some \"input\"})

  Returns a deref-able `WorkflowHandle`: `@handle` blocks for the result."
  [^DBOSClient client wf-key id-or-opts workflow-data]
  (let [{:keys [wf-name class-name]} (core/workflow-identity wf-key)
        {:keys [workflow-id queue] :as opts} (core/->workflow-opts id-or-opts)]
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
                       (core/->enqueue-options (assoc opts
                                                      :workflow-name wf-name
                                                      :class-name class-name))
                       (object-array [(cond-> workflow-data
                                        workflow-id (assoc :workflow/id workflow-id))])))))
