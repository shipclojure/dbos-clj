(ns dbos.dev-logging
  "Points trove — the logging facade the library logs steps through — at
  Telemere, so per-step workflow logs actually show up in the REPL and in test
  output. Without a backend trove is a no-op.

  Lives under test/ because that path is on the classpath for both the :dev
  REPL and the :test runner; it is dev tooling, not a test."
  (:require
   [taoensso.trove :as trove]
   [taoensso.trove.telemere :as trove-telemere]))

(defn install!
  "Wire trove -> Telemere, and make step context available to log calls inside
  step bodies (so a `t/log!` in a step carries its step name).

  Idempotent. Takes and returns an optional argument so it can be used as a
  kaocha hook."
  ([]
   ;; :bridge-ctx? opts into trove merging its ctx into Telemere's native ctx,
   ;; which is what makes a bare t/log! inside a step body carry :workflow/step.
   (trove/set-log-fn! (trove-telemere/get-log-fn {:bridge-ctx? true}))
   nil)
  ([x]
   (install!)
   x))
