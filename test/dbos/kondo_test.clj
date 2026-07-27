(ns dbos.kondo-test
  "Runs the clj-kondo config we publish to consumers
  (resources/clj-kondo.exports/com.shipclojure/dbos-clj) against the fixture in
  lint-fixtures/, and asserts the exact set of findings.

  The fixture is organised one rule per `defn`: every `ok-*` fn must lint clean
  and every `bad-*` fn must raise exactly the linter named below. Findings are
  attributed to their enclosing `defn` rather than to a line number, so the
  fixture can be edited without renumbering this test."
  (:require
   [clj-kondo.core :as clj-kondo]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]))

(def ^:private fixture-file "lint-fixtures/dbos_lint/steps.clj")

(def ^:private config-dir
  "resources/clj-kondo.exports/com.shipclojure/dbos-clj")

(def ^:private expected
  "Fixture fn -> the linter it must trigger. Every other fn must lint clean."
  {"bad-keyword-spec" :dbos-clj/invalid-step
   "bad-nil-spec" :dbos-clj/invalid-step
   "bad-number-spec" :dbos-clj/invalid-step
   "bad-vector-spec" :dbos-clj/invalid-step
   "bad-blank-name" :dbos-clj/invalid-step
   "bad-missing-name-key" :dbos-clj/invalid-step
   "bad-name-not-a-string" :dbos-clj/invalid-step
   "bad-blank-name-key" :dbos-clj/invalid-step

   "bad-unknown-option" :dbos-clj/invalid-step-option
   "bad-max-attempts" :dbos-clj/invalid-step-option
   "bad-backoff-rate" :dbos-clj/invalid-step-option
   "bad-retry-interval" :dbos-clj/invalid-step-option

   "bad-empty-body" :dbos-clj/empty-step-body

   "bad-nested-step" :dbos-clj/nested-step
   "bad-nested-step-under-branch" :dbos-clj/nested-step

   "bad-start-workflow-in-step" :dbos-clj/step-body-violation
   "bad-set-event-in-step" :dbos-clj/step-body-violation
   "bad-workflow-sleep-in-step" :dbos-clj/step-body-violation
   "bad-get-event-in-step" :dbos-clj/step-body-violation})

(defn- defn-index
  "Ascending [line fn-name] pairs for the top-level defns in the fixture."
  []
  (into []
        (keep-indexed
         (fn [idx line]
           (when-let [[_ nm] (re-find #"^\(defn ([^\s\[]+)" line)]
             [(inc idx) nm])))
        (str/split-lines (slurp (io/file fixture-file)))))

(defn- enclosing-defn
  [index row]
  (->> index
       (take-while (fn [[line _]] (<= line row)))
       last
       second))

(defn- lint!
  []
  (:findings
   (clj-kondo/run! {:lint [fixture-file]
                    :config-dir config-dir
                    ;; Hermetic: never read or write the project's lint cache.
                    :cache false})))

(deftest exported-config-flags-every-bad-step-test
  (let [index (defn-index)
        findings (lint!)
        by-fn (reduce (fn [acc {:keys [row type]}]
                        (update acc (enclosing-defn index row) (fnil conj #{}) type))
                      {}
                      findings)]

    (testing "the fixture actually parsed"
      (is (seq index) (str "no defns found in " fixture-file))
      (is (seq findings) "expected the fixture to produce findings"))

    (testing "every bad-* fn raises exactly its linter"
      (doseq [[fn-name linter] (sort expected)]
        (is (= #{linter} (get by-fn fn-name))
            (str fn-name " should raise only " linter))))

    (testing "every ok-* fn lints clean"
      (doseq [[_ fn-name] index
              :when (str/starts-with? fn-name "ok-")]
        (is (nil? (get by-fn fn-name))
            (str fn-name " should not raise " (get by-fn fn-name)))))

    (testing "no findings outside the fixture fns we account for"
      (is (= (set (keys expected))
             (set (keys by-fn)))))))

(deftest exported-config-is-loadable-test
  (testing "every linter the hooks register has a level, or findings are dropped"
    (let [config (read-string (slurp (io/file config-dir "config.edn")))
          hooked (-> config :hooks :analyze-call vals set)
          registered (-> config :linters keys set)]
      (is (seq hooked) "expected :hooks :analyze-call entries")
      (doseq [linter (vals expected)]
        (is (contains? registered linter)
            (str linter " is raised by a hook but has no :linters level"))))))
