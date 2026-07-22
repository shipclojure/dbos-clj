(ns dbos.core-integration-test
  "Live-DBOS tests. Boots a real DBOS instance against a throwaway Postgres
  and exercises the dummy + dummy-parent workflows end-to-end through the
  executor and the transit serializer.

  Proves the durable-execution guarantees (completion, same-id replay,
  implicit parent/child linkage) rather than any domain logic.

  Requires a reachable Postgres. Configure via env vars (defaults in
  parens):
    DBOS_TEST_DATABASE_URL (jdbc:postgresql://localhost:5432/dbos_clj_test)
    DBOS_TEST_DB_USER      (postgres)
    DBOS_TEST_DB_PASSWORD  (postgres)

  DBOS creates its own system schema (:migrate? true). Tagged :integration
  so the default unit suite (which needs no database) stays green."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [dbos.client :as client]
   [dbos.core :as core]
   [dbos.dummy-workflow :as dummy]
   [dbos.query :as query])
  (:import
   (dev.dbos.transact.workflow Queue)))

(def ^:private test-queue-name "dbos-clj-test-queue")

(defn- env [k default] (or (System/getenv k) default))

(defn- test-config []
  {:app-name "dbos-clj-test"
   :executor-id "dbos-clj-test-executor"
   :database-url (env "DBOS_TEST_DATABASE_URL"
                      "jdbc:postgresql://localhost:5432/dbos_clj_test")
   :db-user (env "DBOS_TEST_DB_USER" "postgres")
   :db-password (env "DBOS_TEST_DB_PASSWORD" "postgres")
   :migrate? true})

(def ^:private ^:dynamic *instance* nil)
(def ^:private ^:dynamic *client* nil)

(defn- with-dbos-system [f]
  (let [instance (core/create
                  {:config (test-config)
                   :queues [(-> (Queue. test-queue-name)
                                (.withWorkerConcurrency (int 4)))]
                   :workflows dummy/definitions})
        cfg (test-config)
        the-client (client/create-client
                    {:database-url (:database-url cfg)
                     :db-user (:db-user cfg)
                     :db-password (:db-password cfg)})]
    (core/launch! instance)
    (try
      (binding [*instance* instance
                *client* the-client]
        (f))
      (finally
        (.close the-client)
        (core/shutdown! instance)))))

(use-fixtures :once with-dbos-system)

(defn- start-dummy! [wf-id input]
  (core/start-workflow! *instance* :dbos.dummy/dummy wf-id input))

(defn- start-parent! [wf-id input]
  (core/start-workflow! *instance* :dbos.dummy/dummy-parent wf-id input))

(deftest ^:integration dummy-workflow-test
  (let [wf-id (str "test-dummy-" (random-uuid))
        handle (start-dummy! wf-id {:message "from-test"})
        result @handle]
    (testing "completes and returns the enriched input"
      (is (= "from-test" (:message result)))
      (is (= :completed (:workflow/status result)))
      (is (uuid? (:workflow/stamp-id result)))
      (is (inst? (:workflow/stamped-at result))))

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
                          (core/start-workflow! *instance* :dbos.dummy/nope {} {})))))

(deftest ^:integration client-enqueue-and-query-test
  (let [wf-id (str "test-enqueue-" (random-uuid))
        handle (client/enqueue-workflow! *client* :dbos.dummy/dummy
                                         {:workflow/id wf-id
                                          :workflow/queue test-queue-name}
                                         {:message "enqueued"})
        result @handle]
    (testing "the enqueued workflow runs on the in-process executor"
      (is (= "enqueued" (:message result)))
      (is (= :completed (:workflow/status result)))
      (is (= wf-id (:workflow/id result))))

    (testing "get-workflow-status works via the client"
      (is (= "SUCCESS" (:status (query/get-workflow-status *client* wf-id)))))

    (testing "get-workflow-status works via the in-process instance too"
      (is (= "SUCCESS" (:status (query/get-workflow-status *instance* wf-id)))))

    (testing "list-workflows finds the workflow by id prefix"
      (let [rows (query/list-workflows *client* {:workflow-id-prefix wf-id})]
        (is (= 1 (count rows)))
        (is (= wf-id (:workflow-id (first rows))))
        (is (= "SUCCESS" (:status (first rows))))))))

(deftest ^:integration step-context-propagates-onto-dbos-thread-test
  ;; The real question: DBOS runs the workflow body on its OWN worker thread.
  ;; Does the *step-context-fn* set via set-step-context-fn! (a root value)
  ;; actually establish context on THAT thread, visible deep inside the step
  ;; body? Wire a hook that binds dummy/*ambient-context*, run the probe
  ;; workflow, and assert a read from inside the step saw the step tag.
  (let [test-thread (.getName (Thread/currentThread))
        prev core/*step-context-fn*]
    (try
      (core/set-step-context-fn!
       (fn [ctx f]
         (binding [dummy/*ambient-context* (merge dummy/*ambient-context* ctx)]
           (f))))
      (let [wf-id (str "test-ctx-" (random-uuid))
            result @(core/start-workflow! *instance* :dbos.dummy/context-probe
                                          wf-id {})]
        (testing "context set via set-step-context-fn! reaches deep into the step body"
          (is (= {:workflow/step "observe-context"}
                 (:step/captured-context result))))

        (testing "and it did so on the DBOS worker thread, not the test thread"
          (is (some? (:step/thread result)))
          (is (not= test-thread (:step/thread result)))))
      (finally
        (core/set-step-context-fn! prev)))))
