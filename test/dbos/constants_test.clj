(ns dbos.constants-test
  (:require
   [clojure.set :as set]
   [clojure.test :refer [deftest is testing]]
   [dbos.constants :as const])
  (:import
   (dev.dbos.transact.workflow WorkflowState)))

(deftest status-constants-match-enum-test
  (testing "every status constant is a real WorkflowState enum value"
    (doseq [status const/all-statuses]
      (is (instance? WorkflowState (WorkflowState/valueOf status))
          (str status " should be a valid WorkflowState")))))

(deftest status-sets-test
  (testing "status sets partition the terminal/in-progress statuses correctly"
    (is (= #{"PENDING" "ENQUEUED"} const/in-progress-statuses))
    (is (contains? const/terminal-statuses const/status-success))
    (is (every? const/terminal-statuses const/error-statuses))
    (is (= (into const/in-progress-statuses const/terminal-statuses)
           const/all-statuses))
    (is (empty? (set/intersection const/in-progress-statuses
                                  const/terminal-statuses)))))