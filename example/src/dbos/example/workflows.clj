(ns dbos.example.workflows
  "Example workflows wired as Integrant components. A workflow fn is
  `[deps dbos input]`, partially applied to `deps` at registration so DBOS
  sees the `[dbos input]` shape it expects."
  (:require
   [dbos.core :as dbos :refer [run-step]]
   [integrant.core :as ig]
   [taoensso.telemere :as t])
  (:import
   (java.time Duration Instant)))

(defn dummy-workflow
  "Echo `input` back enriched with durable step values: a stamp, a bare
  progress log, and a short durable sleep."
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
  "Start a single `dummy-workflow` child and wait for it."
  [_deps dbos input]
  (let [handle (dbos/start-workflow!
                dbos :dbos.example/dummy
                {:workflow/id (str (dbos/workflow-id) "|child")}
                input)]
    {:workflow/status :completed
     :child/workflow-id (.workflowId handle)
     :child/result @handle}))

(defn process-item-workflow
  "One unit of the fan-out: durably process a single `:item`."
  [_deps dbos {:keys [item]}]
  (let [result (run-step dbos "process-item"
                         ;; stand-in for real async work (HTTP/DB/compute)
                         {:item item :squared (* item item)})]
    (dbos/workflow-sleep dbos (Duration/ofMillis 25)) ; simulate latency
    (assoc result :processed? true)))

(defn fan-out-workflow
  "Scatter/gather: fan out one `process-item` child per item, await all, then
  aggregate. Children are started up front (sequential `mapv`, never
  pmap/future) so they run concurrently on the queue before we deref any."
  [{:keys [queue]} dbos {:keys [n] :or {n 5}}]
  (let [items   (run-step dbos "gather-input" (vec (range 1 (inc n))))
        parent  (dbos/workflow-id)
        handles (mapv (fn [item]
                        (dbos/start-workflow!
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
  "A scheduled workflow: DBOS invokes it each cron tick with
  {:scheduled/at <Instant> :schedule/context <ctx>} as input."
  [_deps dbos {:scheduled/keys [at]}]
  (run-step dbos "record-heartbeat"
            {:workflow/status :completed
             :heartbeat/at (or at (Instant/now))}))

(def ^:dynamic *ambient-context*
  "Stand-in for a logging backend's ambient context, bound by a *step-context-fn*."
  nil)

(defn context-probe-workflow
  "Return the ambient context + thread visible from inside a step body (proves
  `*step-context-fn*` wraps the whole body on the worker thread)."
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
  {:workflow/key :dbos.example/dummy
   :workflow/fn (partial dummy-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/dummy-parent
  [_ deps]
  {:workflow/key :dbos.example/dummy-parent
   :workflow/fn (partial dummy-parent-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/context-probe
  [_ deps]
  {:workflow/key :dbos.example/context-probe
   :workflow/fn (partial context-probe-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/process-item [_ deps]
  {:workflow/key :dbos.example/process-item
   :workflow/fn (partial process-item-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/fan-out [_ deps]
  {:workflow/key :dbos.example/fan-out
   :workflow/fn (partial fan-out-workflow deps)})

(defmethod ig/init-key :dbos.example.workflow/heartbeat [_ deps]
  {:workflow/key :dbos.example/heartbeat
   :workflow/fn (partial heartbeat-workflow deps)
   :workflow/schedule {:cron "*/2 * * * * *"}}) ; every 2s (SPRING53 6-field)
