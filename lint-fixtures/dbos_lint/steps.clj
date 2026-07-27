(ns dbos-lint.steps
  "Lint fixture for the exported clj-kondo config. Every form below is annotated
  with the finding it must produce (or with the fact that it must be silent).
  `dbos.kondo-test` asserts the exact set — keep the two in sync.

  This directory is NOT on the classpath and is not meant to compile."
  (:require
   [dbos.core :as dbos :refer [do-step! run-step]])
  (:import
   (java.time Duration)))

;; -- Valid: must produce no findings ------------------------------------------

(defn ok-name-string [dbos input]
  (run-step dbos "fetch-user" (:user-id input)))

(defn ok-options-map [dbos input]
  (run-step dbos {:name "fetch-user"
                  :max-attempts 3
                  :retry-interval (Duration/ofSeconds 2)
                  :backoff-rate 2.0
                  :retry? (fn [t] (instance? java.io.IOException t))}
            (:user-id input)))

(defn ok-do-step [dbos input]
  (do-step! dbos "notify" (prn input)))

(defn ok-computed-spec [dbos step-name input]
  ;; Not a literal — nothing to check statically, must stay silent.
  (run-step dbos step-name (:x input))
  (run-step dbos (str "step-" 1) (:x input)))

(defn ok-body-only-calls-in-body [dbos input]
  ;; Outside any step body: fine.
  (dbos/set-event! dbos :progress 1)
  (dbos/workflow-sleep dbos (Duration/ofMillis 50))
  (dbos/start-workflow! dbos :wf/child {:workflow/id "child"} input))

(defn ok-sequential-steps [dbos input]
  ;; Sibling steps are fine — only nesting is not.
  (let [a (run-step dbos "a" (:x input))]
    (do-step! dbos "b" (prn a))
    (run-step dbos "c" a)))

(defn ok-body-still-analyzed [dbos input]
  ;; Locals bound inside a step body must keep resolving normally.
  (run-step dbos "a"
            (let [x (:n input)]
              (* x 2))))

;; -- :dbos-clj/invalid-step ---------------------------------------------------

(defn bad-keyword-spec [dbos]
  ;; error: keyword is not a step spec
  (run-step dbos :fetch-user 1))

(defn bad-nil-spec [dbos]
  ;; error: nil is not a step spec
  (run-step dbos nil 1))

(defn bad-number-spec [dbos]
  ;; error: number is not a step spec
  (do-step! dbos 42 (prn 1)))

(defn bad-vector-spec [dbos]
  ;; error: vector is not a step spec
  (run-step dbos ["a"] 1))

(defn bad-blank-name [dbos]
  ;; error: blank step name
  (run-step dbos "   " 1))

(defn bad-missing-name-key [dbos]
  ;; error: options map without :name
  (run-step dbos {:max-attempts 3} 1))

(defn bad-name-not-a-string [dbos]
  ;; error: :name must be a string
  (run-step dbos {:name :fetch-user} 1))

(defn bad-blank-name-key [dbos]
  ;; error: :name must not be blank
  (run-step dbos {:name ""} 1))

;; -- :dbos-clj/invalid-step-option --------------------------------------------

(defn bad-unknown-option [dbos]
  ;; warning: :max-attemps is a typo and is silently ignored
  (run-step dbos {:name "a" :max-attemps 3} 1))

(defn bad-max-attempts [dbos]
  ;; warning: must be a positive integer
  (run-step dbos {:name "a" :max-attempts 0} 1))

(defn bad-backoff-rate [dbos]
  ;; warning: must be a number
  (run-step dbos {:name "a" :backoff-rate "2.0"} 1))

(defn bad-retry-interval [dbos]
  ;; warning: must be a Duration, not a bare number
  (run-step dbos {:name "a" :retry-interval 2} 1))

;; -- :dbos-clj/empty-step-body ------------------------------------------------

(defn bad-empty-body [dbos]
  ;; warning: step does nothing
  (run-step dbos "a"))

;; -- :dbos-clj/nested-step ----------------------------------------------------

(defn bad-nested-step [dbos input]
  ;; error: step inside a step
  (run-step dbos "outer"
            (run-step dbos "inner" (:x input))))

(defn bad-nested-step-under-branch [dbos input]
  ;; error: still nested, one branch deeper
  (run-step dbos "outer"
            (when (:x input)
              (do-step! dbos "inner" (prn 1)))))

;; -- :dbos-clj/step-body-violation --------------------------------------------

(defn bad-start-workflow-in-step [dbos input]
  ;; error: child workflow started from a step
  (run-step dbos "outer"
            (dbos/start-workflow! dbos :wf/child {:workflow/id "c"} input)))

(defn bad-set-event-in-step [dbos]
  ;; error: event published from a step
  (do-step! dbos "outer"
            (dbos/set-event! dbos :progress 1)))

(defn bad-workflow-sleep-in-step [dbos]
  ;; error: durable sleep from a step
  (run-step dbos "outer"
            (dbos/workflow-sleep dbos (Duration/ofMillis 10))))

(defn bad-get-event-in-step [dbos]
  ;; error: get-event is for callers outside the workflow
  (run-step dbos "outer"
            (dbos/get-event dbos "wf-1" :progress)))
