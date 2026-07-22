(ns dbos.core-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [dbos.core :as core])
  (:import
   (dev.dbos.transact DBOSClient$EnqueueOptions StartWorkflowOptions)
   (dev.dbos.transact.workflow Queue)
   (java.time Duration)))

;; -- Step macros (no live DBOS; execute-step is redefined) --------------------

(deftest run-step-executes-body-test
  (testing "run-step runs the body via execute-step and returns its value"
    (let [calls (atom [])]
      (with-redefs [core/execute-step (fn [_dbos step-name thunk]
                                        (swap! calls conj step-name)
                                        (thunk))]
        (is (= 42 (core/run-step ::dbos "compute" (+ 40 2))))
        (is (= ["compute"] @calls))))))

(deftest do-step-executes-body-test
  (testing "do-step! runs the body via execute-do-step! for side effects"
    (let [effects (atom [])
          seen (atom [])]
      (with-redefs [core/execute-do-step! (fn [_dbos step-name thunk]
                                            (swap! seen conj step-name)
                                            (thunk)
                                            nil)]
        (is (nil? (core/do-step! ::dbos "notify" (swap! effects conj :sent))))
        (is (= [:sent] @effects))
        (is (= ["notify"] @seen))))))

;; -- *step-context-fn* hook ---------------------------------------------------

(deftest step-context-fn-default-noop-test
  (testing "default *step-context-fn* runs the body and returns its value"
    (is (= :ok (core/*step-context-fn* {:workflow/step "x"} (fn [] :ok))))))

(deftest step-context-fn-bound-sees-step-ctx-test
  (testing "a bound *step-context-fn* receives the step ctx map from run-step"
    (let [seen-ctx (atom nil)]
      (binding [core/*step-context-fn* (fn [ctx thunk]
                                         (reset! seen-ctx ctx)
                                         (thunk))]
        (with-redefs [core/execute-step (fn [_dbos _step-name thunk] (thunk))]
          (is (= 7 (core/run-step ::dbos "step-a" 7)))
          (is (= {:workflow/step "step-a"} @seen-ctx)))))))

(deftest step-context-fn-wraps-whole-body-test
  (testing "the step body runs inside the context, so nested reads see it"
    ;; simulate a backend context via a dynamic var the hook binds; a call
    ;; deep in the body observes the step tag — proving downstream tagging.
    (let [ambient (atom nil)]
      (binding [core/*step-context-fn* (fn [ctx thunk]
                                         (reset! ambient ctx)
                                         (thunk))]
        (with-redefs [core/execute-step (fn [_dbos _step-name thunk] (thunk))]
          (let [observed-deep-in-body (core/run-step ::dbos "log-step"
                                                     (deref ambient))]
            (is (= {:workflow/step "log-step"} observed-deep-in-body))))))))

(deftest set-step-context-fn!-test
  (testing "set-step-context-fn! sets the root value seen by run-step"
    (let [prev core/*step-context-fn*
          seen (atom nil)]
      (try
        (core/set-step-context-fn! (fn [ctx thunk] (reset! seen ctx) (thunk)))
        (with-redefs [core/execute-step (fn [_dbos _step-name thunk] (thunk))]
          (core/run-step ::dbos "root-step" :done))
        (is (= {:workflow/step "root-step"} @seen))
        (finally
          (core/set-step-context-fn! prev))))))

;; -- Options builders ---------------------------------------------------------

(deftest ->start-options-test
  (testing "builds a StartWorkflowOptions with the requested fields"
    (let [^StartWorkflowOptions opts (core/->start-options
                                      {:workflow-id "wf-1"
                                       :queue "q"
                                       :timeout (Duration/ofSeconds 5)
                                       :deduplication-id "dd"
                                       :priority 3
                                       :delay (Duration/ofSeconds 1)})]
      (is (instance? StartWorkflowOptions opts))
      (is (= "wf-1" (.workflowId opts)))))

  (testing "empty map yields a bare StartWorkflowOptions"
    (is (instance? StartWorkflowOptions (core/->start-options {})))))

(deftest ->enqueue-options-test
  (testing "builds a DBOSClient$EnqueueOptions targeting the given identity"
    (let [^DBOSClient$EnqueueOptions opts (core/->enqueue-options
                                           {:workflow-name "wf"
                                            :class-name "my.ns"
                                            :queue "q"
                                            :workflow-id "wf-2"
                                            :priority 1})]
      (is (instance? DBOSClient$EnqueueOptions opts)))))

;; -- ->workflow-opts (start/enqueue caller surfaces) --------------------------

(deftest ->workflow-opts-string-form-test
  (testing "a bare string is treated as the workflow-id"
    (is (= "wf-1" (:workflow-id (core/->workflow-opts "wf-1"))))))

(deftest ->workflow-opts-map-form-test
  (testing "a namespaced opts map maps onto the internal option keys"
    (is (= {:workflow-id "wf-1"
            :queue "my-queue"
            :timeout nil
            :deduplication-id "dd"
            :priority 2
            :delay nil}
           (core/->workflow-opts {:workflow/id "wf-1"
                                  :workflow/queue "my-queue"
                                  :workflow/deduplication-id "dd"
                                  :workflow/priority 2})))))

(deftest ->workflow-opts-queue-instance-test
  (testing ":workflow/queue accepts a Queue instance, normalized to its name"
    (is (= "qname"
           (:queue (core/->workflow-opts {:workflow/queue (Queue. "qname")})))))

  (testing "an invalid queue value throws"
    (is (thrown? clojure.lang.ExceptionInfo
                 (core/->workflow-opts {:workflow/queue 42})))))

;; -- workflow-identity --------------------------------------------------------

(deftest workflow-identity-test
  (testing "splits a namespaced keyword into wf-name/class-name"
    (is (= {:wf-name "create-position" :class-name "positions"}
           (core/workflow-identity :positions/create-position)))))

;; -- workflow-definition-errors (pure Clojure) --------------------------------

(deftest workflow-definition-errors-valid-test
  (testing "a minimal valid definition has no errors"
    (is (nil? (core/workflow-definition-errors
               {:workflow/name :my.ns/wf
                :workflow/fn (fn [_dbos _input])}))))

  (testing "optional keys, when well-formed, are accepted"
    (is (nil? (core/workflow-definition-errors
               {:workflow/name :my.ns/wf
                :workflow/fn (fn [_dbos _input])
                :workflow/max-recovery-attempts 3
                :workflow/schedule {:cron "* * * * *" :queue "q"}}))))

  (testing "a schedule without a :queue is valid (queue is optional)"
    (is (nil? (core/workflow-definition-errors
               {:workflow/name :my.ns/wf
                :workflow/fn (fn [_dbos _input])
                :workflow/schedule {:cron "* * * * *"}})))))

(deftest workflow-definition-errors-invalid-test
  (testing "non-namespaced :workflow/name is rejected"
    (is (= [":workflow/name must be a namespaced keyword"]
           (core/workflow-definition-errors
            {:workflow/name :bare
             :workflow/fn (fn [_ _])}))))

  (testing "missing :workflow/fn is rejected"
    (is (some #{":workflow/fn must be a function of [dbos input]"}
              (core/workflow-definition-errors
               {:workflow/name :my.ns/wf}))))

  (testing "non-integer max-recovery-attempts is rejected"
    (is (some #{":workflow/max-recovery-attempts must be an integer"}
              (core/workflow-definition-errors
               {:workflow/name :my.ns/wf
                :workflow/fn (fn [_ _])
                :workflow/max-recovery-attempts "3"}))))

  (testing "malformed :workflow/schedule is rejected (missing :cron)"
    (is (some #{":workflow/schedule must be {:cron string} with an optional :queue string"}
              (core/workflow-definition-errors
               {:workflow/name :my.ns/wf
                :workflow/fn (fn [_ _])
                :workflow/schedule {:queue "q"}}))))

  (testing "malformed :workflow/schedule is rejected (non-string :queue)"
    (is (some #{":workflow/schedule must be {:cron string} with an optional :queue string"}
              (core/workflow-definition-errors
               {:workflow/name :my.ns/wf
                :workflow/fn (fn [_ _])
                :workflow/schedule {:cron "* * * * *" :queue 42}}))))

  (testing "a non-map definition is rejected"
    (is (some #{"definition must be a map"}
              (core/workflow-definition-errors "nope")))))

(deftest validate-workflow!-test
  (testing "returns the definition unchanged when valid"
    (let [def {:workflow/name :my.ns/wf :workflow/fn (fn [_ _])}]
      (is (= def (core/validate-workflow! def)))))

  (testing "throws ex-info carrying :workflow/validation-errors when invalid"
    (try
      (core/validate-workflow! {:workflow/name :bare})
      (is false "expected validate-workflow! to throw")
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (is (= :bare (:workflow/name data)))
          (is (seq (:workflow/validation-errors data))))))))
