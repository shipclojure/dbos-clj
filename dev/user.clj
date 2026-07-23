(ns user)

;; Enable reflection warnings for the whole dev session. This lives here (dev
;; classpath only) rather than in the shipped `dbos.*` namespaces, so consumers
;; loading the library from source don't get our dev-time warnings.
;;
;; `alter-var-root` (not `set!`): the compiler binds `*warn-on-reflection*`
;; only for the duration of loading this file, so a bare `set!` would revert
;; once user.clj finishes. Altering the root value makes it stick across
;; subsequent `(require ... :reload)`s in the REPL.
(alter-var-root #'*warn-on-reflection* (constantly true))
