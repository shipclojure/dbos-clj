(ns dbos.core-integration-test
  "Live-DBOS tests. Boot the minimalistic Integrant example
  (`dbos.example.system`, under example/) against a throwaway Postgres and
  exercise the dummy + dummy-parent workflows end-to-end through the executor
  and the transit serializer — the library used exactly as a real consumer
  wires it, rather than ad-hoc setup.

  Proves the durable-execution guarantees (completion, same-id replay,
  implicit parent/child linkage) and the consumer-facing surfaces (Integrant
  lifecycle, client enqueue/query) rather than any domain logic.

  Requires a reachable Postgres. Configure via env vars (defaults in
  parens):
    DBOS_TEST_DATABASE_URL (jdbc:postgresql://localhost:5432/dbos_clj_test)
    DBOS_TEST_DB_USER      (postgres)
    DBOS_TEST_DB_PASSWORD  (postgres)

  DBOS creates its own system schema. Tagged :integration so the default unit
  suite (which needs no database) stays green."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [dbos.client :as client]
   [dbos.core :as core]
   [dbos.example.serializer :as serializer]
   [dbos.example.system :as system]
   [dbos.query :as query]))

(def ^:private example-queue-name system/queue-name)

(defn- env [k default] (or (System/getenv k) default))

(def ^:private ^:dynamic *instance* nil)
(def ^:private ^:dynamic *client* nil)

(defn- with-example-system [f]
  (let [sys (system/start!)
        the-client (client/create-client
                    {:database-url (env "DBOS_TEST_DATABASE_URL"
                                        "jdbc:postgresql://localhost:5432/dbos_clj_test")
                     :db-user (env "DBOS_TEST_DB_USER" "postgres")
                     :db-password (env "DBOS_TEST_DB_PASSWORD" "postgres")
                     ;; the client must share the instance's serializer so the
                     ;; two agree on the wire format (java.time handlers, etc.)
                     :serializer (serializer/transit-serializer)})]
    (try
      (binding [*instance* (:dbos/instance sys)
                *client* the-client]
        (f))
      (finally
        (.close the-client)
        (system/stop! sys)))))

(use-fixtures :once with-example-system)

(defn- start-dummy! [wf-id input]
  (core/start-workflow! *instance* :dbos.example/dummy wf-id input))

(defn- start-parent! [wf-id input]
  (core/start-workflow! *instance* :dbos.example/dummy-parent wf-id input))

(deftest ^:integration dummy-workflow-test
  (let [wf-id (str "test-dummy-" (random-uuid))
        handle (start-dummy! wf-id {:message "from-test"})
        result @handle]
    (testing "completes and returns the enriched input"
      (is (= "from-test" (:message result)))
      (is (= :completed (:workflow/status result)))
      (is (uuid? (:workflow/stamp-id result)))
      (is (inst? (:workflow/stamped-at result))))

    (testing "the injected java.time handler round-trips a real Instant"
      (is (instance? java.time.Instant (:workflow/stamped-at result))))

    (testing "the handle carries the requested workflow id"
      (is (= wf-id (.workflowId handle))))

    (testing "re-starting the same workflow-id replays the recorded run"
      (let [result-2 @(start-dummy! wf-id {:message "from-test"})]
        (is (= result result-2))
        (is (= (:workflow/stamp-id result) (:workflow/stamp-id result-2)))))

    (testing "a different workflow-id is a fresh execution"
      (let [result-3 @(start-dummy! (str "test-dummy-" (random-uuid))
                                    {:message "from-test"})]
        (is (not= (:workflow/stamp-id result) (:workflow/stamp-id result-3)))))))

(deftest ^:integration child-workflow-test
  (let [wf-id (str "test-parent-" (random-uuid))
        handle (start-parent! wf-id {:message "from-parent"})
        result @handle]
    (testing "parent starts a child and derefs its result inline"
      (is (= :completed (:workflow/status result)))
      (is (= (str wf-id "|child") (:child/workflow-id result)))
      (is (= "from-parent" (get-in result [:child/result :message])))
      (is (= :completed (get-in result [:child/result :workflow/status]))))

    (testing "replaying the parent returns the recorded child, no duplicate"
      (let [result-2 @(start-parent! wf-id {:message "from-parent"})]
        (is (= result result-2))))))

(deftest ^:integration start-workflow-validation-test
  (testing "starting an unregistered workflow name throws synchronously"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Workflow not registered"
                          (core/start-workflow! *instance* :dbos.example/nope {} {})))))

(deftest ^:integration client-enqueue-and-query-test
  (let [wf-id (str "test-enqueue-" (random-uuid))
        handle (client/enqueue-workflow! *client* :dbos.example/dummy
                                         {:workflow/id wf-id
                                          :workflow/queue example-queue-name}
                                         {:message "enqueued"})
        result @handle]
    (testing "the enqueued workflow runs on the in-process executor"
      (is (= "enqueued" (:message result)))
      (is (= :completed (:workflow/status result)))
      ;; the workflow id is NOT injected into the input — it rides on the
      ;; handle instead
      (is (= wf-id (.workflowId handle))))

    (testing "get-workflow-status works via the client"
      (is (= "SUCCESS" (:status (query/get-workflow-status *client* wf-id)))))

    (testing "get-workflow-status works via the in-process instance too"
      (is (= "SUCCESS" (:status (query/get-workflow-status *instance* wf-id)))))

    (testing "list-workflows finds the workflow by id prefix"
      (let [rows (query/list-workflows *client* {:workflow-id-prefix wf-id})]
        (is (= 1 (count rows)))
        (is (= wf-id (:workflow-id (first rows))))
        (is (= "SUCCESS" (:status (first rows))))))))

(deftest ^:integration fan-out-workflow-test
  (let [wf-id (str "test-fanout-" (random-uuid))
        result @(core/start-workflow! *instance* :dbos.example/fan-out wf-id {:n 5})]
    (testing "gathers input, fans out 5 children, aggregates their results"
      (is (= :completed (:workflow/status result)))
      (is (= 5 (:n result)))
      ;; 1^2 + 2^2 + 3^2 + 4^2 + 5^2 = 55
      (is (= 55 (:sum-of-squares result)))
      (is (= 5 (count (:items result))))
      (is (every? :processed? (:items result))))
    (testing "each child ran under a stable per-item id derived from the parent"
      (let [child-id (str wf-id "|item-3")
            status (query/get-workflow-status *instance* child-id)]
        (is (= "SUCCESS" (:status status)))))
    (testing "replaying the parent returns the recorded aggregate, no re-fan-out"
      (is (= result @(core/start-workflow! *instance* :dbos.example/fan-out wf-id {:n 5}))))))

(deftest ^:integration scheduled-heartbeat-test
  (testing "the */2s scheduled heartbeat workflow fires and completes"
    (let [deadline (+ (System/currentTimeMillis) 12000)
          heartbeat? (fn []
                       (seq (query/list-workflows
                             *instance*
                             {:workflow-name "heartbeat" :statuses ["SUCCESS"] :limit 1})))]
      (loop []
        (cond
          (heartbeat?) (is true "observed at least one completed heartbeat run")
          (< (System/currentTimeMillis) deadline) (do (Thread/sleep 500) (recur))
          :else (is false "no heartbeat workflow completed within 12s"))))))
