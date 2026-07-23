(ns dbos.core-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [dbos.core :as core])
  (:import
   (dev.dbos.transact DBOSClient$EnqueueOptions StartWorkflowOptions)
   (dev.dbos.transact.config DBOSConfig)
   (dev.dbos.transact.workflow Queue StepOptions VersionInfo)
   (java.time Duration Instant)))

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

;; -- ->step-options -----------------------------------------------------------

(deftest ->step-options-string-form-test
  (testing "a string becomes a name-only StepOptions with no retry (maxAttempts 1)"
    (let [^StepOptions opts (core/->step-options "fetch")]
      (is (instance? StepOptions opts))
      (is (= "fetch" (.name opts)))
      (is (= 1 (.maxAttempts opts))))))

(deftest ->step-options-map-form-test
  (testing "a map sets the requested retry fields on the StepOptions"
    (let [interval (Duration/ofSeconds 2)
          ^StepOptions opts (core/->step-options
                             {:name "fetch-user"
                              :max-attempts 3
                              :retry-interval interval
                              :backoff-rate 2.0})]
      (is (= "fetch-user" (.name opts)))
      (is (= 3 (.maxAttempts opts)))
      (is (instance? Duration (.retryInterval opts)))
      (is (= interval (.retryInterval opts)))
      (is (= 2.0 (.backOffRate opts))))))

(deftest ->step-options-retry?-test
  (testing ":retry? becomes a Predicate returning the fn's boolean"
    (let [^StepOptions opts (core/->step-options
                             {:name "flaky"
                              :retry? #(instance? IllegalStateException %)})
          pred (.shouldRetry opts)]
      (is (true? (.test pred (IllegalStateException. "retry me"))))
      (is (false? (.test pred (RuntimeException. "nope")))))))

(deftest ->step-options-passthrough-test
  (testing "a pre-built StepOptions is returned unchanged (identical)"
    (let [built (StepOptions. "prebuilt")]
      (is (identical? built (core/->step-options built))))))

(deftest ->step-options-invalid-test
  (testing "a non-name/map/StepOptions input throws"
    (is (thrown? clojure.lang.ExceptionInfo (core/->step-options 42)))))

(deftest ->step-options-blank-name-test
  (testing "a blank or missing step name throws (name is required)"
    (is (thrown? clojure.lang.ExceptionInfo (core/->step-options "")))
    (is (thrown? clojure.lang.ExceptionInfo (core/->step-options "   ")))
    (is (thrown? clojure.lang.ExceptionInfo (core/->step-options {:max-attempts 3})))
    (is (thrown? clojure.lang.ExceptionInfo (core/->step-options {:name "  "})))))

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

(deftest ->start-options-per-field-fidelity-test
  (testing "every requested field round-trips onto the StartWorkflowOptions"
    (let [delay (Duration/ofSeconds 1)
          deadline (Instant/parse "2030-01-01T00:00:00Z")
          ^StartWorkflowOptions opts (core/->start-options
                                      {:workflow-id "wf-1"
                                       :queue "q"
                                       :deduplication-id "dd"
                                       :priority 3
                                       :delay delay
                                       :deadline deadline
                                       :queue-partition-key "pk"
                                       :app-version "v9"})]
      (is (= "wf-1" (.workflowId opts)))
      (is (= "q" (.queueName opts)))
      (is (= "dd" (.deduplicationId opts)))
      (is (= 3 (.priority opts)))
      (is (= delay (.delay opts)))
      (is (= deadline (.deadline opts)))
      (is (= "pk" (.queuePartitionKey opts)))
      (is (= "v9" (.appVersion opts))))))

(deftest ->enqueue-options-per-field-fidelity-test
  (testing "every requested field round-trips onto the DBOSClient$EnqueueOptions"
    (let [^DBOSClient$EnqueueOptions opts (core/->enqueue-options
                                           {:workflow-name "wf"
                                            :class-name "my.ns"
                                            :queue "q"
                                            :workflow-id "wf-2"
                                            :priority 1
                                            :deduplication-id "dd"
                                            :app-version "v9"
                                            :queue-partition-key "pk"})]
      (is (= "wf" (.workflowName opts)))
      (is (= "my.ns" (.className opts)))
      (is (= "q" (.queueName opts)))
      (is (= "wf-2" (.workflowId opts)))
      (is (= 1 (.priority opts)))
      (is (= "dd" (.deduplicationId opts)))
      (is (= "v9" (.appVersion opts)))
      (is (= "pk" (.queuePartitionKey opts))))))

(deftest ->start-options-passthrough-test
  (testing "a pre-built StartWorkflowOptions is returned unchanged (identical)"
    (let [^StartWorkflowOptions built (.withWorkflowId (StartWorkflowOptions.) "wf-x")]
      (is (identical? built (core/->start-options built))))))

(deftest ->enqueue-options-passthrough-test
  (testing "a pre-built DBOSClient$EnqueueOptions is returned unchanged (identical)"
    (let [^DBOSClient$EnqueueOptions built (DBOSClient$EnqueueOptions. "wf" "my.ns" "q")]
      (is (identical? built (core/->enqueue-options built))))))

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
            :delay nil
            :app-version nil
            :deadline nil
            :queue-partition-key nil}
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

(deftest ->workflow-opts-built-passthrough-test
  (testing "a pre-built StartWorkflowOptions is wrapped under ::core/built"
    (let [^StartWorkflowOptions built (.withWorkflowId (StartWorkflowOptions.) "wf-b")
          opts (core/->workflow-opts built)]
      (is (contains? opts :dbos.core/built))
      (is (identical? built (::core/built opts))))))

(deftest ->start-options-on-built-opts-test
  (testing "the built object from ->workflow-opts feeds ->start-options unchanged"
    (let [^StartWorkflowOptions built (.withWorkflowId (StartWorkflowOptions.) "wf-c")
          built-from-opts (::core/built (core/->workflow-opts built))]
      (is (identical? built (core/->start-options built-from-opts))))))

(deftest ->workflow-opts-blank-guard-test
  (testing "blank/empty strings are treated as absent (nil) internal values"
    (let [opts (core/->workflow-opts {:workflow/id ""
                                      :workflow/queue ""
                                      :workflow/deduplication-id ""
                                      :workflow/app-version ""
                                      :workflow/queue-partition-key ""})]
      (is (nil? (:workflow-id opts)))
      (is (nil? (:queue opts)))
      (is (nil? (:deduplication-id opts)))
      (is (nil? (:app-version opts)))
      (is (nil? (:queue-partition-key opts)))))

  (testing "a whitespace-only string is also treated as blank"
    (is (nil? (:workflow-id (core/->workflow-opts {:workflow/id "  "})))))

  (testing "->start-options on an all-blank opts map builds a bare object"
    (let [opts (core/->workflow-opts {:workflow/id ""
                                      :workflow/queue ""
                                      :workflow/deduplication-id ""
                                      :workflow/app-version ""
                                      :workflow/queue-partition-key ""})]
      (is (instance? StartWorkflowOptions (core/->start-options opts))))))

(deftest ->workflow-opts-app-version-test
  (testing "a string app-version lands under :app-version"
    (is (= "v1" (:app-version (core/->workflow-opts {:workflow/app-version "v1"})))))

  (testing "the :latest sentinel passes through unresolved"
    (is (= :latest
           (:app-version (core/->workflow-opts {:workflow/app-version :latest}))))))

(deftest ->workflow-opts-auto-id-test
  (testing "omitting the id (nil or {}) yields a nil :workflow-id (autogen path)"
    (is (nil? (:workflow-id (core/->workflow-opts nil))))
    (is (nil? (:workflow-id (core/->workflow-opts {})))))

  (testing "->start-options on the auto-id opts builds a bare StartWorkflowOptions"
    (is (instance? StartWorkflowOptions
                   (core/->start-options (core/->workflow-opts nil))))
    (is (instance? StartWorkflowOptions
                   (core/->start-options (core/->workflow-opts {}))))))

;; -- resolve-app-version ------------------------------------------------------

(deftest resolve-app-version-test
  (testing "nil resolves to nil (no round-trip)"
    (is (nil? (core/resolve-app-version nil nil))))

  (testing "a plain string resolves to itself"
    (is (= "v9" (core/resolve-app-version nil "v9"))))

  (testing "an invalid value throws ex-info"
    (is (thrown? clojure.lang.ExceptionInfo
                 (core/resolve-app-version nil 42))))

  (testing ":latest resolves to the latest version-id via AppVersioned"
    (let [stub (reify core/AppVersioned
                 (-get-latest-app-version [_]
                   (VersionInfo. "v9" "name" nil nil)))]
      (is (= "v9" (core/resolve-app-version stub :latest)))))

  (testing ":latest is best-effort — nil when there is no latest version"
    (let [stub (reify core/AppVersioned
                 (-get-latest-app-version [_] nil))]
      (is (nil? (core/resolve-app-version stub :latest))))))

;; -- dbos-config (pure; datasource optional) ----------------------------------

(deftest dbos-config-test
  (testing "maps the plain config onto the DBOSConfig fields"
    (let [^DBOSConfig cfg (core/dbos-config {:app-name "test-app"
                                             :app-version "v1"
                                             :admin-server? true})]
      (is (instance? DBOSConfig cfg))
      (is (= "test-app" (.appName cfg)))
      (is (= "v1" (.appVersion cfg)))
      (is (true? (.adminServer cfg))))))

;; -- workflow-identity --------------------------------------------------------

(deftest workflow-identity-test
  (testing "splits a namespaced keyword into wf-name/class-name"
    (is (= {:wf-name "create-position" :class-name "positions"}
           (core/workflow-identity :positions/create-position)))))

;; -- workflow-definition-errors (pure Clojure) --------------------------------

(deftest workflow-definition-errors-valid-test
  (testing "a minimal valid definition has no errors"
    (is (nil? (core/workflow-definition-errors
               {:workflow/key :my.ns/wf
                :workflow/fn (fn [_dbos _input])}))))

  (testing "optional keys, when well-formed, are accepted"
    (is (nil? (core/workflow-definition-errors
               {:workflow/key :my.ns/wf
                :workflow/fn (fn [_dbos _input])
                :workflow/max-recovery-attempts 3
                :workflow/schedule {:cron "* * * * *" :queue "q"}}))))

  (testing "a schedule without a :queue is valid (queue is optional)"
    (is (nil? (core/workflow-definition-errors
               {:workflow/key :my.ns/wf
                :workflow/fn (fn [_dbos _input])
                :workflow/schedule {:cron "* * * * *"}})))))

(deftest workflow-definition-errors-invalid-test
  (testing "non-namespaced :workflow/key is rejected"
    (is (= [":workflow/key must be a namespaced keyword"]
           (core/workflow-definition-errors
            {:workflow/key :bare
             :workflow/fn (fn [_ _])}))))

  (testing "missing :workflow/fn is rejected"
    (is (some #{":workflow/fn must be a function of [dbos input]"}
              (core/workflow-definition-errors
               {:workflow/key :my.ns/wf}))))

  (testing "non-integer max-recovery-attempts is rejected"
    (is (some #{":workflow/max-recovery-attempts must be an integer"}
              (core/workflow-definition-errors
               {:workflow/key :my.ns/wf
                :workflow/fn (fn [_ _])
                :workflow/max-recovery-attempts "3"}))))

  (testing "malformed :workflow/schedule is rejected (missing :cron)"
    (is (some #{":workflow/schedule must be {:cron string} with an optional :queue string"}
              (core/workflow-definition-errors
               {:workflow/key :my.ns/wf
                :workflow/fn (fn [_ _])
                :workflow/schedule {:queue "q"}}))))

  (testing "malformed :workflow/schedule is rejected (non-string :queue)"
    (is (some #{":workflow/schedule must be {:cron string} with an optional :queue string"}
              (core/workflow-definition-errors
               {:workflow/key :my.ns/wf
                :workflow/fn (fn [_ _])
                :workflow/schedule {:cron "* * * * *" :queue 42}}))))

  (testing "a non-map definition is rejected"
    (is (some #{"definition must be a map"}
              (core/workflow-definition-errors "nope")))))

(deftest validate-workflow!-test
  (testing "returns the definition unchanged when valid"
    (let [def {:workflow/key :my.ns/wf :workflow/fn (fn [_ _])}]
      (is (= def (core/validate-workflow! def)))))

  (testing "throws ex-info carrying :workflow/validation-errors when invalid"
    (try
      (core/validate-workflow! {:workflow/key :bare})
      (is false "expected validate-workflow! to throw")
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (is (= :bare (:workflow/key data)))
          (is (seq (:workflow/validation-errors data))))))))
