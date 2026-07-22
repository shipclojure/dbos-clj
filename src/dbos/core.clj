(ns dbos.core
  (:require
   [dbos.serializer :as serializer]
   [taoensso.trove :as trove])
  (:import
   (dev.dbos.transact DBOS DBOSClient$EnqueueOptions StartWorkflowOptions)
   (dev.dbos.transact.config DBOSConfig)
   (dev.dbos.transact.execution ThrowingRunnable ThrowingSupplier)
   (dev.dbos.transact.workflow Queue WorkflowHandle WorkflowSchedule)
   (java.time Duration Instant)))

(def ^:dynamic *step-context-fn*
  "Contextual-logging hook for step bodies: a fn of [ctx-map thunk].
  `run-step`/`do-step!` wrap their whole body in it with
  {:workflow/step <step-name>}, so logs emitted anywhere inside the step
  carry the step name. No-op by default; set with `set-step-context-fn!`.
  As a root value it applies on every thread, including DBOS worker threads."
  (fn [_ctx thunk] (thunk)))

(defn set-step-context-fn!
  "Set the root value of `*step-context-fn*` (see its docstring). Call once at
  startup, next to `taoensso.trove/set-log-fn!`. `f` merges `ctx-map` into your
  backend's ambient context, e.g. (fn [ctx f] (u/with-context ctx (f)))."
  [f]
  (alter-var-root #'*step-context-fn* (constantly f)))

;; -- Step execution -----------------------------------------------------------

(defn execute-step
  "Run a value-returning step via DBOS (result persisted). Indirection layer
  so tests can `with-redefs` it to bypass DBOS persistence."
  [^DBOS dbos step-name thunk]
  (.runStep dbos
            ^ThrowingSupplier (reify ThrowingSupplier
                                (execute [_] (thunk)))
            ^String step-name))

(defn execute-do-step!
  "Run a side-effect-only step via DBOS (result NOT persisted). Indirection
  layer so tests can `with-redefs` it to bypass DBOS persistence."
  [^DBOS dbos step-name thunk]
  (.runStep dbos
            ^ThrowingRunnable (reify ThrowingRunnable
                                (execute [_] (thunk) nil))
            ^String step-name))

(defmacro run-step
  "Run a workflow step that RETURNS a value, persisted for durability/recovery.
  Use for non-deterministic work (ids, timestamps, API calls) the workflow
  reuses later; keep results small (they are serialized). Takes the DBOS
  instance first. The whole body runs inside `*step-context-fn*` with
  {:workflow/step step-name}.

  (run-step dbos \"fetch-user-data\" (call-external-api user-id))"
  [dbos step-name & body]
  `(let [step-name# ~step-name]
     (*step-context-fn*
      {:workflow/step step-name#}
      (fn []
        (trove/log! {:level :debug
                     :id :dbos.workflow/step-start
                     :data {:workflow/step step-name#}})
        (execute-step ~dbos step-name# (fn [] ~@body))))))

(defmacro do-step!
  "Run a workflow step for SIDE-EFFECTS only. The result is NOT persisted —
  cheaper than `run-step` when unused (DB writes, notifications). Takes the
  DBOS instance first. The whole body runs inside `*step-context-fn*` with
  {:workflow/step step-name}.

  (do-step! dbos \"send-notification\" (http/post url {:body data}))"
  [dbos step-name & body]
  `(let [step-name# ~step-name]
     (*step-context-fn*
      {:workflow/step step-name#}
      (fn []
        (trove/log! {:level :debug
                     :id :dbos.workflow/step-start
                     :data {:workflow/step step-name#}})
        (execute-do-step! ~dbos step-name# (fn [] ~@body))))))

(defn workflow-sleep
  "Durably sleep the current workflow. Survives restarts: the wake-up time
  is persisted, not the thread. Wrapped so tests can `with-redefs` it."
  [^DBOS dbos ^Duration duration]
  (.sleep dbos duration))

;; -- Events (generic key/value channel; progress is one use) ------------------

(defn set-event!
  "Publish `value` under `event-key` on the running workflow (a generic
  key/value channel; workflow progress is one use). Body-only — DBOS.setEvent
  throws from a step or outside a workflow. Last write wins; durable and
  idempotent under replay, so no run-step/do-step! wrapping is needed."
  [^DBOS dbos event-key value]
  (.setEvent dbos (name event-key) value))

(defn get-event
  "Read the latest value published under `event-key` for `workflow-id` (read
  side of `set-event!`). Call from OUTSIDE a workflow. Non-blocking: returns
  nil immediately when nothing has been published yet."
  [^DBOS dbos workflow-id event-key]
  (.orElse (.getEvent dbos workflow-id (name event-key) Duration/ZERO)
           nil))

;; -- Options builders ---------------------------------------------------------

(defn ->start-options
  "Build StartWorkflowOptions from a map:
  - `:workflow-id` - The id for the workflow instance,
  - `:queue` (name, optional),
  - `:timeout` (Duration),
  - `:deduplication-id` (optional) - ID used to deduplicate the workflow. Separate than `:workflow-id`,
  - `:priority` (int, lower runs first),
  - `:delay` (Duration)."
  ^StartWorkflowOptions
  [{:keys [workflow-id queue timeout deduplication-id priority]
    delay-duration :delay}]
  (cond-> (StartWorkflowOptions.)
    workflow-id (.withWorkflowId ^String workflow-id)
    queue (.withQueue ^String queue)
    timeout (.withTimeout ^Duration timeout)
    deduplication-id (.withDeduplicationId ^String deduplication-id)
    priority (.withPriority (int priority))
    delay-duration (.withDelay ^Duration delay-duration)))

(defn ->enqueue-options
  "Build DBOSClient$EnqueueOptions (for enqueuing via the client) from a map:
  - `:workflow-name` (required),
  - `:class-name` (required - the DBOS className the worker registered under),
  - `:queue` (name, optional),
  - `:workflow-id` - The id for the workflow instance,
  - `:timeout` (Duration),
  - `:deduplication-id` (optional) - ID used to deduplicate the workflow. Separate than `:workflow-id`,
  - `:priority` (int, lower runs first),
  - `:delay` (Duration).

  The (:workflow-name, :class-name) pair typically comes from
  `workflow-identity`, so enqueue targets the exact (workflowName, className)
  the worker registered under."
  ^DBOSClient$EnqueueOptions
  [{:keys [workflow-name class-name queue workflow-id timeout deduplication-id priority]
    delay-duration :delay}]
  (cond-> (DBOSClient$EnqueueOptions. ^String workflow-name ^String class-name ^String queue)
    workflow-id (.withWorkflowId ^String workflow-id)
    timeout (.withTimeout ^Duration timeout)
    deduplication-id (.withDeduplicationId ^String deduplication-id)
    priority (.withPriority (int priority))
    delay-duration (.withDelay ^Duration delay-duration)))

(defn- queue-name
  "The queue name string from either a name string or a Queue instance."
  [queue]
  (cond
    (nil? queue) nil
    (string? queue) queue
    (instance? Queue queue) (.name ^Queue queue)
    :else (throw (ex-info "queue must be a name string or a Queue instance"
                          {:queue queue}))))

(defn ->workflow-opts
  "Normalize the id-or-opts arg accepted by `start-workflow!` /
  `enqueue-workflow!` into the internal options map the builders take. Accepts:
  - a bare workflow-id string, or
  - a map keyed with :workflow/id, :workflow/queue (name string or Queue
    instance), :workflow/timeout, :workflow/deduplication-id,
    :workflow/priority, :workflow/delay."
  [id-or-opts]
  (if (string? id-or-opts)
    {:workflow-id id-or-opts}
    (let [{:workflow/keys [id queue timeout deduplication-id priority]
           delay-duration :workflow/delay} id-or-opts]
      {:workflow-id id
       :queue (queue-name queue)
       :timeout timeout
       :deduplication-id deduplication-id
       :priority priority
       :delay delay-duration})))

;; -- Handles + identity -------------------------------------------------------

(defn add-derefable
  "Wrap a WorkflowHandle so it also implements IDeref for Clojure
  ergonomics: @handle blocks and returns the workflow result (already
  deserialized by the transit serializer)."
  [^WorkflowHandle handle]
  (reify
    clojure.lang.IDeref
    (deref [_] (.getResult handle))

    WorkflowHandle
    (workflowId [_] (.workflowId handle))
    (getResult [_] (.getResult handle))
    (getStatus [_] (.getStatus handle))))

(defn workflow-identity
  "Split a workflow keyword into the {:wf-name .. :class-name ..} pair DBOS
  registers and looks workflows up by (name -> workflowName, namespace ->
  className). The single source of truth shared by registration and lookup,
  so the two can never drift."
  [wf-name]
  {:wf-name (name wf-name)
   :class-name (namespace wf-name)})

(defn workflow-id
  "The id of the currently executing workflow (ambient thread-local context).
  Call from INSIDE a workflow body; nil when none is running on this thread.
  Handy for child ids, e.g. (str (workflow-id) \"|child\")."
  []
  (DBOS/workflowId))

(defn start-child-workflow!
  "Start a registered workflow from INSIDE another workflow's body (workflow
  fns close over `dbos` alone). Parent linkage is recorded implicitly by DBOS.

  Call from the workflow body only — never inside a step — and on the
  workflow's own thread in deterministic order (sequential reduce/mapv, never
  pmap/future); replay depends on it.

  - `dbos`    the raw DBOS instance (as passed into the workflow fn)
  - `wf-name` keyword the child workflow was registered under
  - `opts`    StartWorkflowOptions map, see ->start-options
  - `input`   the child's single serializable workflow argument"
  [^DBOS dbos wf-name opts input]
  (let [integration (.integration dbos)
        {:keys [wf-name class-name]} (workflow-identity wf-name)
        registered (.orElse (.getRegisteredWorkflow integration wf-name class-name)
                            nil)]
    (when-not registered
      (throw (ex-info "Workflow not registered" {:workflow/name wf-name})))
    (add-derefable
     (.startRegisteredWorkflow integration registered (object-array [input])
                               (->start-options opts)))))

;; -- Configuration ------------------------------------------------------------

(defn dbos-config
  "Build a DBOSConfig from a plain map. :app-name plus a database source are
  required; everything else is optional and generalizes both consumers.

  Provide the database either as a ready DataSource, or as a JDBC url that
  DBOS opens itself:

  - :datasource     any `javax.sql.DataSource` DBOS persists to (e.g. a
                    HikariCP pool)
  - :database-url   a JDBC url (alternative to :datasource)
  - :db-user        optional db user, paired with :database-url
  - :db-password    optional db password, paired with :database-url
  - :migrate?       optional; let DBOS create/upgrade its system schema

  Everything else:
  - :app-name       DBOS application name (required)
  - :serializer     a DBOSSerializer; defaults to the transit serializer with
                    no injected handlers (see dbos.serializer). Pass one built
                    with your app-wide transit handlers for java.time fidelity.
  - :app-version    optional app version string
  - :executor-id    optional executor id (defaults to DBOS's own)
  - :schema         optional database schema name
  - :listen-queues  optional seq of queue names this executor listens to
  - :admin-server?  optional; enable the DBOS admin HTTP server
  - :conductor      optional {:domain .. :api-key ..} for DBOS Conductor"
  ^DBOSConfig
  [{:keys [datasource database-url db-user db-password migrate?
           app-name app-version executor-id serializer
           schema listen-queues admin-server? conductor]}]
  (cond-> (-> (DBOSConfig/defaults app-name)
              (.withSerializer (or serializer (serializer/transit-serializer))))
    datasource          (.withDataSource datasource)
    database-url        (.withDatabaseUrl database-url)
    db-user             (.withDbUser db-user)
    db-password         (.withDbPassword db-password)
    (some? migrate?)    (.withMigrate (boolean migrate?))
    executor-id         (.withExecutorId executor-id)
    app-version         (.withAppVersion app-version)
    schema              (.withDatabaseSchema schema)
    (seq listen-queues) (.withListenQueues (into-array String listen-queues))
    admin-server?       (.withAdminServer true)
    (:domain conductor)  (.withConductorDomain (:domain conductor))
    (:api-key conductor) (.withConductorKey (:api-key conductor))))

;; -- Workflow definition validation (pure Clojure, no Malli) ------------------

(defn workflow-definition-errors
  "Return a vector of human-readable problems with a workflow definition,
  or nil when it's valid. Pure Clojure — no schema library.

  A workflow definition is a map:
  - :workflow/name                 namespaced keyword (required)
  - :workflow/fn                   fn of [dbos input] (required)
  - :workflow/max-recovery-attempts optional integer
  - :workflow/schedule             optional {:cron string} with an optional
                                   :queue string"
  [{wf-name :workflow/name
    wf-fn :workflow/fn
    max-attempts :workflow/max-recovery-attempts
    schedule :workflow/schedule
    :as definition}]
  (not-empty
   (cond-> []
     (not (map? definition))
     (conj "definition must be a map")

     (not (and (keyword? wf-name) (namespace wf-name)))
     (conj ":workflow/name must be a namespaced keyword")

     (not (fn? wf-fn))
     (conj ":workflow/fn must be a function of [dbos input]")

     (and (some? max-attempts) (not (int? max-attempts)))
     (conj ":workflow/max-recovery-attempts must be an integer")

     (and (some? schedule)
          (not (and (map? schedule)
                    (string? (:cron schedule))
                    (or (nil? (:queue schedule))
                        (string? (:queue schedule))))))
     (conj (str ":workflow/schedule must be {:cron string} with an optional "
                ":queue string")))))

(defn validate-workflow!
  "Throws ex-info with the collected errors when `definition` is invalid;
  returns it unchanged when valid."
  [definition]
  (when-let [errors (workflow-definition-errors definition)]
    (throw (ex-info "Invalid workflow definition"
                    {:workflow/name (:workflow/name definition)
                     :workflow/validation-errors errors})))
  definition)

;; -- Scheduled-workflow bridge ------------------------------------------------

(definterface ScheduledWorkflowFn
  ;; The 1.0.0 SchedulerService refuses to fire a schedule unless the
  ;; registered workflow Method has the exact (Instant, Object) parameter
  ;; types (SchedulerService/EXPECTED_PARAMETERS — on mismatch it logs
  ;; "invalid signature" and silently never starts the schedule), so
  ;; scheduled workflows can't be registered via IFn.invoke(Object) like the
  ;; regular ones.
  (call [^java.time.Instant fireTime ^Object context]))

(defn scheduled-workflow-target
  "Registration bridge for scheduled workflows. The scheduler invokes the
  workflow with [cron-fire-time schedule-context] (see SchedulerService$1);
  pack them into the single input map shape workflow fns expect."
  [instance wf-fn]
  (reify ScheduledWorkflowFn
    (call [_ fire-time context]
      (wf-fn instance {:scheduled/at fire-time
                       :schedule/context context}))))

;; -- Registration -------------------------------------------------------------

(defn register-workflow!
  "Register a Clojure fn as a named DBOS workflow. Must run before launch of
  passed DBOS instance. Returns the definition with :workflow/registered
  attached (needed by `DBOS/startRegisteredWorkflow`)."
  [^DBOS instance {wf-key :workflow/name
                   wf-fn :workflow/fn
                   schedule :workflow/schedule
                   max-recovery-attempts :workflow/max-recovery-attempts
                   :as workflow}]
  (validate-workflow! workflow)
  (let [{:keys [wf-name class-name]} (workflow-identity wf-key)
        target (if schedule
                 (scheduled-workflow-target instance wf-fn)
                 (fn [input] (wf-fn instance input)))
        invoke-method (if schedule
                        (.getMethod ScheduledWorkflowFn "call"
                                    (into-array Class [Instant Object]))
                        (.getMethod clojure.lang.IFn "invoke"
                                    (into-array Class [Object])))
        registered (.registerWorkflow (.integration instance)
                                      wf-name
                                      class-name
                                      nil
                                      target
                                      invoke-method
                                      (some-> max-recovery-attempts int)
                                      nil)]
    (assoc workflow :workflow/registered registered)))

(defn register-workflows!
  "Register all workflow definitions, returning a registry map of
  :workflow/name -> definition (with :workflow/registered attached).
  Must run before launch."
  [^DBOS instance workflows]
  (into {}
        (map (juxt :workflow/name (partial register-workflow! instance)))
        workflows))

;; -- Lifecycle ----------------------------------------------------------------

(defn create
  "Construct the DBOS instance, register queues and workflows. Does NOT
  launch — the caller controls when the executor goes live via `launch!`.

  - :config    a map for `dbos-config` (see its docstring)
  - :queues    a seq of `dev.dbos.transact.workflow.Queue` to register
  - :workflows a seq of workflow definition maps to register

  Returns the raw DBOS instance — pass it directly to `launch!`,
  `start-workflow!`, `apply-schedules!` and `shutdown!`."
  ^DBOS
  [{:keys [config queues workflows]}]
  (let [instance (DBOS. (dbos-config config))]
    (doseq [^Queue q queues] (.registerQueue instance q))
    (register-workflows! instance workflows)
    instance))

(defn launch!
  "Launch the DBOS executor. Call after workflows are registered on the instance."
  [^DBOS dbos]
  (.launch dbos))

(defn shutdown!
  "Shut down the DBOS executor, draining in-flight workflows."
  [^DBOS dbos]
  (.shutdown dbos))

(defn start-workflow!
  "Start a registered workflow by name, returning a deref-able WorkflowHandle
  (@handle blocks for the result).

  - `dbos`          the DBOS instance returned by `create`
  - `workflow-name` keyword the workflow was registered under
  - `id-or-opts`    a workflow-id string, or an options map (see
                    `->workflow-opts`) — pass a stable id for idempotent starts
  - `input`         the single workflow argument, a serializable map — the
                    only thing persisted for recovery (deps and the DBOS
                    instance are closed over at registration)

  (start-workflow! dbos :my/wf \"wf-1\" {:some \"input\"})
  (start-workflow! dbos :my/wf {:workflow/id \"wf-1\" :workflow/queue q} {:some \"input\"})"
  [^DBOS dbos workflow-name id-or-opts input]
  (let [integration (.integration dbos)
        {:keys [wf-name class-name]} (workflow-identity workflow-name)
        registered (-> (.getRegisteredWorkflow integration wf-name class-name)
                       (.orElse nil))]
    (when-not registered
      (throw (ex-info "Workflow not registered"
                      {:workflow/name workflow-name})))

    (add-derefable
     (.startRegisteredWorkflow integration
                               registered
                               (object-array [input])
                               (->start-options (->workflow-opts id-or-opts))))))

(defn apply-schedules!
  "Upsert a DB-backed WorkflowSchedule row for every definition carrying a
  :workflow/schedule. Call AFTER launch, on the DBOS instance from `create`.
  No-op when no definition is scheduled.

  A schedule may optionally target a queue (:queue in :workflow/schedule);
  when it does, run this only on the executor(s) meant to fire the scheduled
  workflows (whose registration included the scheduled definitions and which
  listen to the target queue). Without a queue the schedule fires on the
  registering executor directly."
  [^DBOS dbos definitions]
  (let [->schedule (fn [{wf-key :workflow/name
                         {:keys [cron queue]} :workflow/schedule}]
                     (let [{:keys [wf-name class-name]} (workflow-identity wf-key)
                           schedule (WorkflowSchedule. wf-name wf-name class-name cron)]
                       (cond-> schedule
                         queue (.withQueueName queue))))
        schedules (into []
                        (comp (filter :workflow/schedule)
                              (map ->schedule))
                        definitions)]
    (when (seq schedules)
      (.applySchedules dbos ^java.util.List schedules))))
