(ns dbos.dummy-workflow
  "Minimal workflows validating the DBOS plumbing end-to-end. They are the
  library's own reference for how plain-fn workflows are written and the
  subjects of the live-DBOS integration test (dbos.core-integration-test).

  - `dummy-workflow` echoes its input enriched with values produced durably
    in steps (a stamp step, a side-effect step, a durable sleep), returning
    a plain map the transit serializer persists.
  - `dummy-parent-workflow` starts a single `dummy-workflow` child and waits
    for it, exercising the parent/child fan-out (`start-child-workflow!`).

  A workflow fn is `[dbos input]`: `dbos` is the DBOS instance closed over at
  registration, `input` is the plain workflow argument map."
  (:require
   [dbos.core :as core :refer [do-step! run-step]])
  (:import
   (java.time Duration Instant)))

(defn dummy-workflow
  "Echo `input` back enriched with values produced durably in steps: a stamp
  (non-deterministic id + timestamp, persisted so recovery replays it), a
  side-effect log step, and a short durable sleep."
  [dbos input]
  (let [stamped (run-step dbos "stamp-input"
                          (assoc input
                                 :workflow/stamped-at (Instant/now)
                                 :workflow/stamp-id (random-uuid)))]
    (do-step! dbos "log-progress"
      ;; side effect only; result is not persisted
              (str "running " (:message input)))
    (core/workflow-sleep dbos (Duration/ofMillis 50))
    (run-step dbos "summarize"
              (assoc stamped :workflow/status :completed))))

(defn dummy-parent-workflow
  "Start a single `dummy-workflow` child and wait for it. The child id is
  derived from the parent's own id (`<parent-id>|child`) purely for
  discoverability - correctness of the parent/child link does not depend on
  it (DBOS records it via the ambient workflow context)."
  [dbos input]
  (let [handle (core/start-child-workflow!
                dbos
                :dbos.dummy/dummy
                {:workflow-id (str (core/workflow-id) "|child")}
                input)]
    {:workflow/status :completed
     :child/workflow-id (.workflowId handle)
     :child/result @handle}))

(def ^:dynamic *ambient-context*
  "Test hook standing in for a logging backend's ambient context. A
  *step-context-fn* can bind this so a step body can observe the context the
  hook established on the DBOS execution thread."
  nil)

(defn context-probe-workflow
  "Return the ambient context visible from deep inside a step body, plus the
  executing thread name — proving `*step-context-fn*` wraps the whole step
  body on the DBOS worker thread (not just the outer step-start log)."
  [dbos _input]
  (run-step dbos "observe-context"
    ;; a call further down in the body reads whatever context the hook set
            {:step/captured-context *ambient-context*
             :step/thread (.getName (Thread/currentThread))}))

(def definitions
  [{:workflow/name :dbos.dummy/dummy
    :workflow/fn dummy-workflow}
   {:workflow/name :dbos.dummy/dummy-parent
    :workflow/fn dummy-parent-workflow}
   {:workflow/name :dbos.dummy/context-probe
    :workflow/fn context-probe-workflow}])
