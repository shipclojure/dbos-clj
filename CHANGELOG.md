# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- REPL dev mode: pass the sentinel `:dbos.core/dev` instead of a live DBOS instance and workflow bodies just run — no Postgres, no `create`/`launch!`. Step bodies execute inline (step options still validated, `do-step!` still returns nil, failures throw immediately with no retries), `workflow-sleep` becomes a plain logged `Thread/sleep`, and `set-event!` only logs. See "REPL-driven development" in the README.
- An AI agent skill under `skills/dbos-clj-workflows/` — teaches coding assistants (Claude Code, ECA, ...) the durable-execution model, step rules and dispatch patterns for writing correct dbos-clj workflows. See the "AI agent skill" section in the README.

## [0.5.0] - 2026-07-27

### Added

- clj-kondo linters for the step macros, shipped in the jar. Import them with `clj-kondo --lint "$(clojure -Spath)" --dependencies --copy-configs`.

  | Linter | Level | Catches |
  | --- | --- | --- |
  | `:dbos-clj/invalid-step` | error | Step spec that is not a name string, options map or `StepOptions`; blank step name; options map without `:name`. |
  | `:dbos-clj/invalid-step-option` | warning | Unknown option keys — `->step-options` drops them, so `:max-attemps` silently disables retries — and wrong-typed option values. |
  | `:dbos-clj/empty-step-body` | warning | Step with no body. |
  | `:dbos-clj/nested-step` | error | Step inside another step. |
  | `:dbos-clj/step-body-violation` | error | `start-workflow!`, `set-event!`, `workflow-sleep` or `get-event` inside a step body. |

  Also `:ret` type annotations for `dbos.core`, so `execute-do-step!`'s nil return and the collection types of `register-workflow(s)!` / `list-app-versions` flow into the type checker.

  Hooks only add findings, so step bodies keep their normal analysis.

### Changed

- Pin clj-kondo `2026.07.24` in `.mise.toml`, matching the new `clj-kondo/clj-kondo` test dependency.

## [0.4.0] - 2026-07-26

### Changed

- Step context now goes through [trove](https://github.com/taoensso/trove)'s own context API instead of a `dbos-clj`-specific hook. Every step body runs inside `trove/with-ctx+ {:workflow/step "<name>"}`, so any `trove/log!` in a step carries the step name with no setup at all.
- **Requires trove 1.2.0 or later**, which is where that context API landed.
- **Breaking:** the `:step/start` log no longer carries `:data {:workflow/step "<name>"}`. The step name now rides in trove's `:ctx`, alongside every other log emitted in the step body, so read it from `(:ctx payload)` instead. Anything matching on `:data` goes quietly nil.

### Removed

- **Breaking:** `set-step-ctx-wrapper!` and the `*step-ctx-wrapper*` dynamic var. To tag *native* backend calls — a bare Telemere `t/log!`, a `μ/log`, an MDC-aware SLF4J layout — opt into trove's context bridge when building your log-fn instead:

  ```clojure
  (trove/set-log-fn! (trove-telemere/get-log-fn {:bridge-ctx? true}))
  ```

  One line where there were two, and the step name reaches your backend's own context without `dbos-clj` knowing which backend you picked. Bridging is supported by the Telemere, Timbre, μ/log and SLF4J (MDC-capable provider) backends.

## [0.3.0] - 2026-07-25

### Added

- `dbos.query/workflow-tree` returns a workflow with its steps and every child workflow expanded in place, recursively — the whole durable execution as one value, for debugging a fan-out at the REPL. Inputs and outputs come back as real Clojure data. Takes `{:max-depth n}` (default 10).

## [0.2.1] - 2026-07-25

### Fixed

- The functions returning a workflow handle — `start-workflow!`, `resume-workflow!`, `add-derefable`, and the client's `enqueue-workflow!` and `retrieve-workflow` — now carry a `^WorkflowHandle` return hint, so calling `.workflowId` (or any other handle method) on their result no longer emits a reflection warning.

## [0.2.0] - 2026-07-25

### Added

- Per-step logging through [trove](https://github.com/taoensso/trove): `run-step` and `do-step!` emit a `:step/start` log carrying `{:workflow/step "<name>"}`. Point trove at your own backend with `trove/set-log-fn!`.
- `set-step-ctx-wrapper!` (and the `*step-ctx-wrapper*` dynamic var) to bridge step context into a logging backend's *native* scope, so a bare `t/log!` or `μ/log` call inside a step body inherits `:workflow/step`. Defaults to a no-op.
- `AppVersioned` is now `:extend-via-metadata`, so a plain map carrying the method in its metadata can stand in for a DBOS instance or client. Useful for stubbing without a live database, and unlike `reify` it survives re-evaluating `dbos.core` in a REPL.

### Fixed

- Documentation: the trove backend example referenced a `taoensso.trove.x` namespace that does not exist; it now names a real backend. Two code samples with unbalanced parentheses were repaired, one of which nested three steps inside each other instead of running them in sequence.

## [0.1.0] - 2026-07-23

### Added

- Initial release: a Clojure wrapper over [dbos-transact-java](https://github.com/dbos-inc/dbos-transact-java) for durable workflows backed by PostgreSQL.
- `dbos.core` — instance lifecycle (`create`, `launch!`, `shutdown!`), workflow registration, `start-workflow!`, the `run-step` and `do-step!` macros with retry options, child workflows, scheduled workflows via `apply-schedules!`, durable events (`set-event!`, `get-event`), `workflow-sleep`, and `cancel-workflow!` / `resume-workflow!`.
- `dbos.client` — `create-client`, `enqueue-workflow!` and `retrieve-workflow`, for dispatching work to out-of-process executors.
- `dbos.query` — `get-workflow-status`, `list-workflows` and `list-workflow-steps`, working against either a DBOS instance or a client.
- `dbos.serializer` — a Transit serializer (recorded per row as `transit_json_verbose`) replacing DBOS's Jackson default, which cannot deserialize Clojure's persistent data structures. Supports custom read/write handlers, and boxes otherwise unhandled types rather than failing silently.
- `dbos.constants` — status strings and status sets as `.cljc`, shareable with a ClojureScript UI.
- Application-version targeting, including resolving `:latest` at dispatch time.

[unreleased]: https://github.com/shipclojure/dbos-clj/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/shipclojure/dbos-clj/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/shipclojure/dbos-clj/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/shipclojure/dbos-clj/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/shipclojure/dbos-clj/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/shipclojure/dbos-clj/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/shipclojure/dbos-clj/releases/tag/v0.1.0
