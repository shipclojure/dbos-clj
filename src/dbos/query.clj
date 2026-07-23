(ns dbos.query
  "Read-side of DBOS: list workflows and get a single workflow's status.

  `getWorkflowStatus`/`listWorkflows` exist with identical signatures on both
  `DBOS` (in-process) and `DBOSClient` (out-of-process) but share no common
  Java interface, so this ns exposes a protocol extended onto both. Public fns
  take Clojure maps in and return Clojure maps out."
  (:import
   (dev.dbos.transact DBOS DBOSClient)
   (dev.dbos.transact.workflow ErrorResult
                               ListWorkflowsInput
                               StepInfo
                               WorkflowState
                               WorkflowStatus)))

(defn ->list-workflows-input
  "Convert a Clojure map to a ListWorkflowsInput object.

   Supported keys:
   - :workflow-ids       - Collection of workflow UUIDs to query
   - :workflow-name      - Filter by workflow function name
   - :statuses           - Collection of status strings (PENDING, ERROR, etc.)
   - :status             - Single status string
   - :queue-name         - Filter by queue name
   - :executor-ids       - Collection of executor IDs
   - :start-time         - OffsetDateTime for created_at >= filter
   - :end-time           - OffsetDateTime for created_at <= filter
   - :limit              - Max results to return
   - :offset             - Pagination offset
   - :sort-desc?         - Sort by creation time descending
   - :load-input?        - Include workflow inputs in result
   - :load-output?       - Include workflow outputs in result
   - :workflow-id-prefix - Filter by workflow-id prefix"
  ^ListWorkflowsInput
  [m]
  (let [statuses (concat (:statuses m)
                         (when-let [s (:status m)] [s]))
        states (mapv #(WorkflowState/valueOf %) statuses)]
    (cond-> (ListWorkflowsInput.)
      (:workflow-ids m) (.withWorkflowIds ^java.util.List (vec (:workflow-ids m)))
      (:workflow-name m) (.withWorkflowName ^String (:workflow-name m))
      (seq states) (.withStatus ^java.util.List states)
      (:queue-name m) (.withQueueName ^String (:queue-name m))
      (:executor-ids m) (.withExecutorIds ^java.util.List (vec (:executor-ids m)))
      (:start-time m) (.withStartTime (:start-time m))
      (:end-time m) (.withEndTime (:end-time m))
      (:limit m) (.withLimit (int (:limit m)))
      (:offset m) (.withOffset (int (:offset m)))
      (some? (:sort-desc? m)) (.withSortDesc (:sort-desc? m))
      (some? (:load-input? m)) (.withLoadInput (:load-input? m))
      (some? (:load-output? m)) (.withLoadOutput (:load-output? m))
      (:workflow-id-prefix m) (.withWorkflowIdPrefix ^String (:workflow-id-prefix m)))))

(defn- get-error-message
  [err]
  (when err
    (if (instance? ErrorResult err)
      (.message ^ErrorResult err)
      (str err))))

(defn workflow-status->map
  "Convert a WorkflowStatus record to a Clojure map."
  [^WorkflowStatus ws]
  (when ws
    {:workflow-id (.workflowId ws)
     :parent-workflow-id (.parentWorkflowId ws)
     :status (str (.status ws))
     :workflow-name (.workflowName ws)
     :class-name (.className ws)
     :executor-id (.executorId ws)
     :created-at (.createdAt ws)
     :updated-at (.updatedAt ws)
     :app-version (.appVersion ws)
     :recovery-attempts (.recoveryAttempts ws)
     :queue-name (.queueName ws)
     :timeout-ms (.timeoutMs ws)
     :deadline-epoch-ms (.deadlineEpochMs ws)
     :started-at-epoch-ms (.startedAtEpochMs ws)
     :priority (.priority ws)
     :input (some-> (.input ws) vec)
     :output (.output ws)
     :error (get-error-message (.error ws))}))

(defn step-info->map
  "Convert a StepInfo record to a Clojure map, or nil for nil."
  [^StepInfo si]
  (when si
    {:function-id (.functionId si)
     :function-name (.functionName si)
     :output (.output si)
     :error (get-error-message (.error si))
     :child-workflow-id (.childWorkflowId si)
     :started-at (.startedAt si)
     :completed-at (.completedAt si)
     :started-at-epoch-ms (.startedAtEpochMs si)
     :completed-at-epoch-ms (.completedAtEpochMs si)
     :serialization (.serialization si)}))

(defprotocol WorkflowQueryable
  "Read-side of DBOS, satisfied by both a DBOS instance and a DBOSClient."
  (-list-workflows [this input]
    "List workflows matching the given ListWorkflowsInput.")
  (-get-workflow-status [this workflow-id]
    "Return the Optional<WorkflowStatus> for the given workflow id.")
  (-list-workflow-steps [this workflow-id]
    "Return the List<StepInfo> recorded for the given workflow id."))

(extend-protocol WorkflowQueryable
  DBOS
  (-list-workflows [this input] (.listWorkflows ^DBOS this input))
  (-get-workflow-status [this workflow-id] (.getWorkflowStatus ^DBOS this workflow-id))
  (-list-workflow-steps [this workflow-id]
    (.listWorkflowSteps ^DBOS this ^String workflow-id))

  DBOSClient
  (-list-workflows [this input] (.listWorkflows ^DBOSClient this input))
  (-get-workflow-status [this workflow-id] (.getWorkflowStatus ^DBOSClient this workflow-id))
  (-list-workflow-steps [this workflow-id]
    (.listWorkflowSteps ^DBOSClient this ^String workflow-id)))

(defn list-workflows
  "Query workflows from DBOS with optional filters, returning a vector of
  status maps. `queryable` is either a DBOS instance or a DBOSClient — the
  same call works on both.

   Example:
   (list-workflows client
                   {:workflow-ids [\"hello-123\" \"hello-345\"]
                    :workflow-name \"bulk-create-opening-child-workflow\"
                    :statuses [\"PENDING\" \"ERROR\"]})"
  [queryable query-params]
  (->> (->list-workflows-input query-params)
       (-list-workflows queryable)
       (mapv workflow-status->map)))

(defn get-workflow-status
  "Get the status map of a single workflow by its ID, or nil if not found.
  `queryable` is either a DBOS instance or a DBOSClient."
  [queryable workflow-id]
  (some-> ^java.util.Optional (-get-workflow-status queryable workflow-id)
          (.orElse nil)
          workflow-status->map))

(defn list-workflow-steps
  "List the recorded steps of a workflow (by id) as a vector of maps, in
  execution order. `queryable` is either a DBOS instance or a DBOSClient.

  Each step map has :function-id, :function-name, :output, :error,
  :child-workflow-id, :started-at, :completed-at, :started-at-epoch-ms,
  :completed-at-epoch-ms, and :serialization."
  [queryable workflow-id]
  (mapv step-info->map (-list-workflow-steps queryable workflow-id)))
