# dbos-clj example

A very minimalistic [Integrant](https://github.com/weavejester/integrant) +
[Telemere](https://github.com/taoensso/telemere) app that uses `dbos-clj`.

It exists so the library's live-DBOS integration tests (`test/dbos/
core_integration_test.clj`) exercise the code through a realistic consumer
wiring instead of ad-hoc setup. The root `:test` alias puts `example/src` and
`example/resources` on the classpath (see `../deps.edn`).

## Layout

| File | Role |
| ---- | ---- |
| `resources/dbos_example/system.edn` | Integrant config. Workflows are keys deriving `:dbos/workflow`; `:dbos/instance` collects them via `#ig/refset :dbos/workflow`. |
| `src/dbos/example/workflows.clj` | `dummy` / `dummy-parent` / `process-item` / `fan-out` / `heartbeat`, each an Integrant workflow component with dependency injection. |
| `src/dbos/example/system.clj` | `:example/datasource` (HikariCP) and `:dbos/instance` components; wires; `start!` / `stop!`. |
| `src/dbos/example/serializer.clj` | The library transit serializer, pre-wired with the app's java.time handlers. |
| `src/dbos/example/transit_time.clj` | A tiny java.time transit handler set (stand-in for an app-wide one). |

## The pattern

```clojure
(require '[dbos.example.system :as system]
         '[dbos.core :as dbos])

(def sys (system/start!))                 ; boots Postgres pool + DBOS
(def instance (:dbos/instance sys))       ; the raw DBOS instance

@(dbos/start-workflow! instance :dbos.example/dummy "wf-1" {:message "hi"})
;; => {:message "hi" :workflow/status :completed :workflow/stamp-id #uuid ...}

(system/stop! sys)
```

- **Workflows as Integrant components** — each definition is a key deriving
  `:dbos/workflow`; `#ig/refset :dbos/workflow` hands the whole set to
  `:dbos/instance`. Dependencies (here a `:greeting`) are injected via the
  key's config and closed over the workflow fn.
- **`:dbos/instance` is the raw DBOS instance** returned by
  `dbos.core/create` — pass it straight to `start-workflow!`.

## Running the integration tests

Needs a reachable Postgres (DBOS builds its own schema). Defaults:
`jdbc:postgresql://localhost:5432/dbos_clj_test`, user/password `postgres`;
override with `DBOS_TEST_DATABASE_URL` / `DBOS_TEST_DB_USER` /
`DBOS_TEST_DB_PASSWORD`.

```bash
# from the repo root
clojure -M:test integration
```
