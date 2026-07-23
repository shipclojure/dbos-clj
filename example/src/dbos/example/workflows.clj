(ns dbos.example.workflows
  "Example workflows wired as Integrant components — the digital-worker
  pattern in miniature.

  Each workflow definition is its own Integrant key deriving :dbos/workflow;
  `:dbos/instance` collects them via `#ig/refset :dbos/workflow`. The key's
  init config is the dependency-injection point: deps (here a `:greeting`
  string) arrive through `ig/init-key` and are closed over the workflow fn,
  since a workflow's own arguments are serialized for recovery and can't
  carry live resources.

  A workflow fn is `[deps dbos input]`, partially applied to `deps` at
  registration so DBOS sees the `[dbos input]` shape it expects (see
  dbos.core/register-workflow!)."
  (:require
   [dbos.core :as dbos :refer [do-step! run-step]]
   [integrant.core :as ig]
   [taoensso.telemere :as t])
  (:import
   (java.time Duration Instant)))

(defn dummy-workflow
  "Echo `input` back enriched with values produced durably in steps: a stamp
  (non-deterministic id + timestamp, persisted so recovery replays it), a
  bare progress log that uses the injected `:greeting`, and a short durable
  sleep.

  Note the log is emitted BARE, not inside a step. A step runs at most once
  and is skipped on replay/recovery, so wrapping a log in `run-step`/`do-step!`
  would silence it on every re-run — the opposite of what a log is for. Only
  non-deterministic *data* the workflow reuses (the stamp, the summary) goes
  in a step."
  [{:keys [greeting]} dbos input]
  (let [stamped (run-step dbos "stamp-input"
                          (assoc input
                                 :workflow/stamped-at (Instant/now)
                                 :workflow/stamp-id (random-uuid)))]
    ;; Logging is not a step — bare, between steps, so it fires on every run.
    (t/log! {:id :example.dummy/progress
             :data {:input input}}
            (str greeting ", " (:message input)))
    (dbos/workflow-sleep dbos (Duration/ofMillis 50))
    (run-step dbos "summarize"
              (assoc stamped :workflow/status :completed))))

(defn dummy-parent-workflow
  "Start a single `dummy-workflow` child and wait for it, exercising the
  parent/child fan-out via `start-child-workflow!`. The child id is derived
  from the parent's own id (`<parent-id>|child`) purely for discoverability
  — DBOS records the real parent/child link via the ambient workflow
  context, not this id."
  [_deps dbos input]
  (let [handle (dbos/start-child-workflow!
                dbos :dbos.example/dummy
                {:workflow/id (str (dbos/workflow-id) "|child")}
                input)]
    {:workflow/status :completed
     :child/workflow-id (.workflowId handle)
     :child/result @handle}))

(defn process-item-workflow
  "One unit of the fan-out: durably process a single `:item` and return its
  result. `fan-out-workflow` starts many of these on the queue so they run
  concurrently. The `run-step` result is persisted, so recovery replays it
  instead of re-doing the work."
  [_deps dbos {:keys [item]}]
  (let [result (run-step dbos "process-item"
                         ;; stand-in for real async work (HTTP/DB/compute)
                         {:item item :squared (* item item)})]
    (dbos/workflow-sleep dbos (Duration/ofMillis 25)) ; simulate latency
    (assoc result :processed? true)))

(defn fan-out-workflow
  "Gather initial input, fan out one `process-item` child per item, await all
  of them, then aggregate — the map/reduce (scatter/gather) pattern.

  The children are STARTED up front: each `start-child-workflow!` returns a
  handle immediately without blocking, so by the time we deref the first one
  the rest are already running on the `:queue`'s worker pool. Parallelism
  comes from starting every child before awaiting any — NOT from threading
  the start calls. The start calls themselves stay sequential and
  deterministic (`mapv`, never `pmap`/`future`); DBOS replay depends on that
  ordering. Stable per-item child ids make the child starts idempotent under
  parent replay."
  [{:keys [queue]} dbos {:keys [n] :or {n 5}}]
  (let [items   (run-step dbos "gather-input" (vec (range 1 (inc n))))
        parent  (dbos/workflow-id)
        handles (mapv (fn [item]
                        (dbos/start-child-workflow!
                         dbos :dbos.example/process-item
                         {:workflow/id (str parent "|item-" item)
                          :workflow/queue queue}
                         {:item item}))
                      items)
        results (mapv deref handles)]
    (run-step dbos "aggregate"
              {:workflow/status :completed
               :n (count results)
               :sum-of-squares (reduce + (map :squared results))
               :items results})))

(defn heartbeat-workflow
  "A scheduled workflow. DBOS invokes it on each cron tick with
  {:scheduled/at <Instant> :schedule/context <ctx>} as its input. This one
  records a durable heartbeat; a real scheduled job might sweep a table,
  expire sessions, or emit metrics. The cron is SPRING53 6-field
  (second minute hour day-of-month month day-of-week)."
  [_deps dbos {:scheduled/keys [at]}]
  (run-step dbos "record-heartbeat"
            {:workflow/status :completed
             :heartbeat/at (or at (Instant/now))}))

(def ^:dynamic *ambient-context*
  "Stands in for a logging backend's ambient context. A *step-context-fn*
  can bind this so a step body can observe the context established on the
  DBOS execution thread (see dbos.example.system/configure-logging! and the
  step-context integration test)."
  nil)

(defn context-probe-workflow
  "Return the ambient context and thread visible from deep inside a step
  body — proof that `*step-context-fn*` wraps the whole step body on the
  DBOS worker thread, not just the outer step-start log."
  [_deps dbos _input]
  (run-step dbos "observe-context"
            {:step/captured-context *ambient-context*
             :step/thread (.getName (Thread/currentThread))}))

;; -- Integrant workflow components -------------------------------------------

(derive :dbos.example.workflow/dummy :dbos/workflow)
(derive :dbos.example.workflow/dummy-parent :dbos/workflow)
(derive :dbos.example.workflow/context-probe :dbos/workflow)
(derive :dbos.example.workflow/process-item :dbos/workflow)
(derive :dbos.example.workflow/fan-out :dbos/workflow)
(derive :dbos.example.workflow/heartbeat :dbos/workflow)

(defmethod ig/init-key :dbos.example.workflow/dummy
  [_ deps]
  {:workflow/name :dbos.example/dummy
   :workflow/fn (partial dummy-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/dummy-parent
  [_ deps]
  {:workflow/name :dbos.example/dummy-parent
   :workflow/fn (partial dummy-parent-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/context-probe
  [_ deps]
  {:workflow/name :dbos.example/context-probe
   :workflow/fn (partial context-probe-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/process-item [_ deps]
  {:workflow/name :dbos.example/process-item
   :workflow/fn (partial process-item-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/fan-out [_ deps]
  {:workflow/name :dbos.example/fan-out
   :workflow/fn (partial fan-out-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/heartbeat [_ deps]
  {:workflow/name :dbos.example/heartbeat
   :workflow/fn (partial heartbeat-workflow deps)
   :workflow/schedule {:cron "*/2 * * * * *"}}) ; every 2s (SPRING53 6-field)
