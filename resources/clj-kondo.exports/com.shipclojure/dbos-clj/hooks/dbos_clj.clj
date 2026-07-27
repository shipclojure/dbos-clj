(ns hooks.dbos-clj
  "clj-kondo hooks for dbos-clj's step macros and workflow-body-only calls.

  Two families of rules:

  - Step specs. `run-step`/`do-step!` take a step name string, an options map,
    or a pre-built StepOptions. Anything else throws from `->step-options` at
    runtime, and an unknown key in the options map is silently dropped by its
    destructuring — so a typo like `:max-attemps` quietly disables retries.
    Both are worth catching statically.

  - Workflow-body-only calls. DBOS matches recorded step results by workflow and
    step ordinal. Nesting a step inside another step, or starting a child
    workflow / publishing an event / sleeping durably from inside a step body,
    breaks replay.

  Every hook returns its node unchanged: these rules only add findings, they
  never reshape code. clj-kondo already analyzes each argument of these
  macros as an expression, so step bodies keep their normal analysis —
  arities, unresolved symbols, unused bindings and type inference on locals
  all still apply."
  (:require
   [clj-kondo.hooks-api :as api]
   [clojure.string :as str]))

;; -- Shared helpers -----------------------------------------------------------

(def ^:private step-macros
  "Step macro names as they appear on the callstack (all in `dbos.core`)."
  '#{run-step do-step!})

(def ^:private step-option-keys
  "Keys `dbos.core/->step-options` destructures. Anything else is dropped."
  #{:name :max-attempts :retry-interval :backoff-rate :retry?})

(def ^:private unknown
  "Marker for a node whose value is only knowable at runtime."
  ::unknown)

(defn- finding!
  [node type message]
  (api/reg-finding! (assoc (meta node) :type type :message message)))

(defn- literal-value
  "The value of `node` when it is a literal we can reason about, else `unknown`.
  Symbols and calls are left alone — they resolve at runtime."
  [node]
  (cond
    (api/string-node? node) (api/sexpr node)
    (api/keyword-node? node) (api/sexpr node)
    (api/token-node? node) (let [v (api/sexpr node)]
                             (if (symbol? v) unknown v))
    :else unknown))

(defn- enclosing-step
  "Name of the step macro this call sits inside, or nil when it is not in a step
  body. `api/callstack` starts at the immediate parent, so a step macro never
  reports itself."
  []
  (some (fn [frame]
          (when (and (= 'dbos.core (:ns frame))
                     (contains? step-macros (:name frame)))
            (:name frame)))
        (api/callstack)))

;; -- Step options map ---------------------------------------------------------

(defn- lint-name! [node]
  (let [v (literal-value node)]
    (cond
      (= unknown v) nil

      (not (string? v))
      (finding! node :dbos-clj/invalid-step
                (str "Step :name must be a string, got " (pr-str v) "."))

      (str/blank? v)
      (finding! node :dbos-clj/invalid-step
                "Step :name must not be blank — StepOptions rejects it."))))

(defn- lint-max-attempts! [node]
  (let [v (literal-value node)]
    (when-not (or (= unknown v)
                  (and (int? v) (pos? v)))
      (finding! node :dbos-clj/invalid-step-option
                (str "Step :max-attempts must be a positive integer, got "
                     (pr-str v) ".")))))

(defn- lint-backoff-rate! [node]
  (let [v (literal-value node)]
    (when-not (or (= unknown v) (number? v))
      (finding! node :dbos-clj/invalid-step-option
                (str "Step :backoff-rate must be a number, got " (pr-str v) ".")))))

(defn- lint-retry-interval! [node]
  (let [v (literal-value node)]
    (when (number? v)
      (finding! node :dbos-clj/invalid-step-option
                (str "Step :retry-interval must be a java.time.Duration, got the "
                     "number " (pr-str v)
                     ". Use e.g. (java.time.Duration/ofSeconds 2).")))))

(defn- lint-options-map! [node]
  (let [entries (partition 2 (:children node))
        present (into #{}
                      (comp (map first)
                            (filter api/keyword-node?)
                            (map api/sexpr))
                      entries)]
    (when-not (contains? present :name)
      (finding! node :dbos-clj/invalid-step
                "Step options map is missing the required :name key."))
    (doseq [[k v] entries
            :when (api/keyword-node? k)]
      (case (api/sexpr k)
        :name (lint-name! v)
        :max-attempts (lint-max-attempts! v)
        :backoff-rate (lint-backoff-rate! v)
        :retry-interval (lint-retry-interval! v)
        :retry? nil
        (finding! k :dbos-clj/invalid-step-option
                  (str "Unknown step option " (api/sexpr k)
                       " — it is silently ignored. Known options: "
                       (str/join ", " (sort step-option-keys)) "."))))))

;; -- Step spec ----------------------------------------------------------------

(defn- never-a-step
  "A description of `node` when it is a literal that can never be a valid step
  spec, else nil."
  [node]
  (cond
    (api/keyword-node? node) "a keyword"
    (api/vector-node? node) "a vector"
    (= :set (api/tag node)) "a set"
    (api/token-node? node) (let [v (api/sexpr node)]
                             (cond
                               (nil? v) "nil"
                               (number? v) "a number"
                               (boolean? v) "a boolean"))))

(defn- lint-step-spec! [node macro-name]
  (cond
    (api/map-node? node)
    (lint-options-map! node)

    (api/string-node? node)
    (when (str/blank? (api/sexpr node))
      (finding! node :dbos-clj/invalid-step
                "Step name must not be blank — StepOptions rejects it."))

    :else
    (when-let [what (never-a-step node)]
      (finding! node :dbos-clj/invalid-step
                (str "The argument after `dbos` to `" macro-name
                     "` must be a step name string, an options map or a "
                     "StepOptions, got " what ".")))))

;; -- Step macros --------------------------------------------------------------

(defn- lint-step! [{:keys [node]} macro-name]
  (let [[_ dbos step & body] (:children node)]
    (cond
      (nil? dbos)
      (finding! node :dbos-clj/invalid-step
                (str "`" macro-name "` takes a DBOS instance, a step spec and a body."))

      (nil? step)
      (finding! node :dbos-clj/invalid-step
                (str "`" macro-name "` is missing its step spec (a name string, "
                     "an options map or a StepOptions)."))

      :else
      (do
        (lint-step-spec! step macro-name)

        (when (empty? body)
          (finding! node :dbos-clj/empty-step-body
                    (str "`" macro-name "` has an empty body — the step is recorded "
                         "but does nothing.")))

        (when-let [outer (enclosing-step)]
          (finding! node :dbos-clj/nested-step
                    (str "`" macro-name "` is nested inside `" outer
                         "`. DBOS steps cannot be nested — run them one after "
                         "another in the workflow body.")))))
    ;; Returned unchanged on purpose. clj-kondo already analyzes every argument
    ;; of these macros as an expression, and keeping the original node is what
    ;; puts `dbos.core/run-step` on the callstack for `enclosing-step` to find.
    ;; Rewriting to a `let` would replace that frame with `clojure.core/let`.
    {:node node}))

(defn run-step [ctx] (lint-step! ctx "run-step"))
(defn do-step! [ctx] (lint-step! ctx "do-step!"))

;; -- Workflow-body-only calls -------------------------------------------------

(defn- lint-body-only! [{:keys [node]} fn-name reason]
  (when-let [outer (enclosing-step)]
    (finding! node :dbos-clj/step-body-violation
              (str "`" fn-name "` must be called from the workflow body, not from "
                   "inside `" outer "` — " reason ".")))
  {:node node})

(defn start-workflow! [ctx]
  (lint-body-only! ctx "start-workflow!"
                   (str "a step body is skipped on replay, so the child would "
                        "never be started again and parent linkage is lost")))

(defn set-event! [ctx]
  (lint-body-only! ctx "set-event!"
                   "events are published by the workflow, not by a step"))

(defn workflow-sleep [ctx]
  (lint-body-only! ctx "workflow-sleep"
                   (str "the durable wake-up time is recorded on the workflow, "
                        "and a completed step is never re-entered")))

(defn get-event [ctx]
  (lint-body-only! ctx "get-event"
                   (str "it is a non-blocking read meant for callers outside the "
                        "workflow")))
