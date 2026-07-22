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
  side-effect log step that uses the injected `:greeting`, and a short
  durable sleep."
  [{:keys [greeting]} dbos input]
  (let [stamped (run-step dbos "stamp-input"
                          (assoc input
                                 :workflow/stamped-at (Instant/now)
                                 :workflow/stamp-id (random-uuid)))]
    (do-step! dbos "log-progress"
              (t/log! {:id :example.dummy/progress
                       :data {:input input}}
                      (str greeting ", " (:message input))))
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
                {:workflow-id (str (dbos/workflow-id) "|child")}
                input)]
    {:workflow/status :completed
     :child/workflow-id (.workflowId handle)
     :child/result @handle}))

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
