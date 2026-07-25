(ns dbos.query-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.walk :as walk]
   [dbos.core :as core]
   [dbos.query :as query]
   [dbos.test-system :as test-system :refer [*instance*]])
  (:import
   (dev.dbos.transact DBOS DBOSClient)
   (dev.dbos.transact.workflow ListWorkflowsInput)))

;; The live-DBOS tests below need the example system; the pure ones above it
;; don't, but a :once fixture is cheap enough to share.
(use-fixtures :once test-system/with-example-system)

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

;; -- whole-shape assertions --------------------------------------------------
;;
;; The tree tests below assert the ENTIRE structure rather than poking at
;; individual keys, so they double as documentation of what `workflow-tree`
;; hands back. Values that differ run to run are dropped first, and the
;; generated workflow id is rewritten to "<wf>".

(def ^:private volatile-workflow-keys
  [:executor-id :app-version :created-at :updated-at :started-at-epoch-ms
   :deadline-epoch-ms])

(def ^:private volatile-step-keys
  [:started-at :completed-at :started-at-epoch-ms :completed-at-epoch-ms])

(declare scrub)

(defn- scrub-id [wf-id s]
  (some-> s (str/replace wf-id "<wf>")))

(defn- scrub-values
  "Replace run-to-run values with stable markers, anywhere in an input/output —
  including workflow ids, which show up inside outputs too."
  [wf-id x]
  (walk/postwalk
   (fn [form]
     (cond
       (map-entry? form) (case (key form)
                           :workflow/stamp-id [(key form) "<uuid>"]
                           :workflow/stamped-at [(key form) "<instant>"]
                           form)
       (string? form) (scrub-id wf-id form)
       :else form))
   x))

(defn- scrub-steps [wf-id steps]
  (mapv (fn [{:keys [function-name] :as step}]
          (cond-> (apply dissoc step volatile-step-keys)
            true (update :child-workflow-id #(scrub-id wf-id %))
            true (update :output #(scrub-values wf-id %))
            ;; a durable sleep records its wake-up time as the step output
            (= "DBOS.sleep" function-name) (assoc :output "<epoch-ms>")
            (:child-workflow step) (update :child-workflow #(scrub wf-id %))))
        steps))

(defn- scrub
  "Drop run-to-run noise and normalize ids so the whole tree can be compared."
  [wf-id wf]
  (-> (apply dissoc wf volatile-workflow-keys)
      (update :workflow-id #(scrub-id wf-id %))
      (update :parent-workflow-id #(scrub-id wf-id %))
      (update :input #(scrub-values wf-id %))
      (update :output #(scrub-values wf-id %))
      (update :steps #(scrub-steps wf-id %))))

(defn- step
  "A step map with the keys `list-workflow-steps` always returns."
  [id name output & {:as extra}]
  (merge {:function-id id
          :function-name name
          :output output
          :error nil
          :child-workflow-id nil
          :serialization nil}
         extra))

(defn- process-item-child
  "The whole expanded tree of one `process-item` child."
  [item]
  {:workflow-id (str "<wf>|item-" item)
   :workflow-name "process-item"
   :class-name "dbos.example"
   :parent-workflow-id "<wf>"
   :status "SUCCESS"
   :error nil
   :input [{:item item}]
   :output {:item item :squared (* item item) :processed? true}
   :queue-name "dbos-example-queue"
   :priority 0
   :recovery-attempts 1
   :timeout-ms nil
   :steps [(step 0 "process-item" {:item item :squared (* item item)})
           (step 1 "DBOS.sleep" "<epoch-ms>")]})

(def ^:private fan-out-3-result
  {:workflow/status :completed
   :n 3
   :sum-of-squares 14
   :items [{:item 1 :squared 1 :processed? true}
           {:item 2 :squared 4 :processed? true}
           {:item 3 :squared 9 :processed? true}]})

(def ^:private dummy-result
  {:message "from-parent"
   :workflow/stamped-at "<instant>"
   :workflow/stamp-id "<uuid>"
   :workflow/status :completed})

(deftest ^:integration workflow-tree-test
  (let [wf-id (str "test-tree-" (random-uuid))
        _ @(core/start-workflow! *instance* :dbos.example/fan-out wf-id {:n 3})]

    (testing "the whole fan-out — parent, steps and children — as one value"
      (is (= {:workflow-id "<wf>"
              :workflow-name "fan-out"
              :class-name "dbos.example"
              :parent-workflow-id nil
              :status "SUCCESS"
              :error nil
              :input [{:n 3}]
              :output fan-out-3-result
              :queue-name nil
              :priority 0
              :recovery-attempts 1
              :timeout-ms nil
              :steps
              [(step 0 "gather-input" [1 2 3])
               ;; each child is expanded at the step that started it ...
               (step 1 "process-item" nil
                     :child-workflow-id "<wf>|item-1"
                     :child-workflow (process-item-child 1))
               (step 2 "process-item" nil
                     :child-workflow-id "<wf>|item-2"
                     :child-workflow (process-item-child 2))
               (step 3 "process-item" nil
                     :child-workflow-id "<wf>|item-3"
                     :child-workflow (process-item-child 3))
               ;; ... and not again where it was awaited, even though the
               ;; awaiting step names the same child
               (step 4 "DBOS.getResult" {:item 1 :squared 1 :processed? true}
                     :child-workflow-id "<wf>|item-1")
               (step 5 "DBOS.getResult" {:item 2 :squared 4 :processed? true}
                     :child-workflow-id "<wf>|item-2")
               (step 6 "DBOS.getResult" {:item 3 :squared 9 :processed? true}
                     :child-workflow-id "<wf>|item-3")
               (step 7 "aggregate" fan-out-3-result)]}
             (scrub wf-id (query/workflow-tree *instance* wf-id)))))

    (testing ":max-depth stops expansion but keeps every child id"
      (let [shallow (query/workflow-tree *instance* wf-id {:max-depth 0})]
        (is (empty? (keep :child-workflow (:steps shallow))))
        (is (= 6 (count (keep :child-workflow-id (:steps shallow)))))))

    (testing "an unknown workflow id is nil, not an exception"
      (is (nil? (query/workflow-tree *instance* "no-such-workflow"))))))

(deftest ^:integration workflow-tree-grandchildren-test
  (testing "expansion recurses into a child's own steps"
    (let [wf-id (str "test-tree-nested-" (random-uuid))
          _ @(core/start-workflow! *instance* :dbos.example/dummy-parent wf-id
                                   {:message "from-parent"})]
      (is (= {:workflow-id "<wf>"
              :workflow-name "dummy-parent"
              :class-name "dbos.example"
              :parent-workflow-id nil
              :status "SUCCESS"
              :error nil
              :input [{:message "from-parent"}]
              :output {:workflow/status :completed
                       :child/workflow-id "<wf>|child"
                       :child/result dummy-result}
              :queue-name nil
              :priority 0
              :recovery-attempts 1
              :timeout-ms nil
              :steps
              [(step 0 "dummy" nil
                     :child-workflow-id "<wf>|child"
                     :child-workflow
                     {:workflow-id "<wf>|child"
                      :workflow-name "dummy"
                      :class-name "dbos.example"
                      :parent-workflow-id "<wf>"
                      :status "SUCCESS"
                      :error nil
                      :input [{:message "from-parent"}]
                      :output dummy-result
                      :queue-name nil
                      :priority 0
                      :recovery-attempts 1
                      :timeout-ms nil
                      ;; the durable workflow-sleep is recorded as a step too
                      :steps [(step 0 "stamp-input"
                                    {:message "from-parent"
                                     :workflow/stamped-at "<instant>"
                                     :workflow/stamp-id "<uuid>"})
                              (step 1 "DBOS.sleep" "<epoch-ms>")
                              (step 2 "summarize" dummy-result)]})
               (step 1 "DBOS.getResult" dummy-result
                     :child-workflow-id "<wf>|child")]}
             (scrub wf-id (query/workflow-tree *instance* wf-id)))))))
