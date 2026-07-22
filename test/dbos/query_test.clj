(ns dbos.query-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [dbos.query :as query])
  (:import
   (dev.dbos.transact DBOS DBOSClient)
   (dev.dbos.transact.workflow ListWorkflowsInput)))

(deftest ->list-workflows-input-test
  (testing "builds a ListWorkflowsInput from a Clojure map"
    (is (instance? ListWorkflowsInput
                   (query/->list-workflows-input
                    {:workflow-ids ["a" "b"]
                     :workflow-name "wf"
                     :statuses ["PENDING" "SUCCESS"]
                     :queue-name "q"
                     :limit 10
                     :offset 0
                     :sort-desc? true
                     :workflow-id-prefix "pref"}))))

  (testing "an empty map yields a bare ListWorkflowsInput"
    (is (instance? ListWorkflowsInput (query/->list-workflows-input {}))))

  (testing "an unknown status string throws (WorkflowState/valueOf)"
    (is (thrown? IllegalArgumentException
                 (query/->list-workflows-input {:status "NOT_A_STATE"})))))

(deftest protocol-extended-onto-both-handles-test
  (testing "WorkflowQueryable is extended onto both DBOS and DBOSClient"
    (is (extends? query/WorkflowQueryable DBOS))
    (is (extends? query/WorkflowQueryable DBOSClient))))

(deftest workflow-status->map-nil-test
  (testing "nil WorkflowStatus maps to nil"
    (is (nil? (query/workflow-status->map nil)))))
