# dbos-clj Operations: Serialization, Testing, Linting, Logging, Storage

Contents:
- [Serialization](#serialization)
- [Testing workflows](#testing-workflows)
- [clj-kondo linting](#clj-kondo-linting)
- [Logging with trove](#logging-with-trove)
- [Storage schema & debugging](#storage-schema--debugging)

## Serialization

Workflow inputs, outputs, errors and step results are serialized to Postgres. `dbos-clj` replaces DBOS's default Jackson serializer (which mutates persistent data structures in place and throws on Clojure data) with a **Transit `json-verbose`** serializer. Plain Clojure data (keywords, UUIDs, collections) round-trips out of the box.

The format name `transit_json_verbose` is recorded per row and is **frozen** — never change a serializer's `name` once live workflow data exists (it's how DBOS picks the reader).

### Injecting transit handlers

No custom handlers are bundled, so e.g. `java.time` values lack first-class fidelity. Pass app-wide handlers:

```clojure
(require '[dbos.serializer :as serializer])

(def ser (serializer/transit-serializer
          {:write-handlers my.app.transit/write-handlers
           :read-handlers  my.app.transit/read-handlers}))

(dbos/create {:config {:app-name "my-app" :datasource db :serializer ser} ...})
;; the client must use the SAME serializer:
(client/create-client {:datasource db :serializer ser})
```

### The java-object box (unhandled types)

A value with no transit handler is boxed as `{:java-object/class .. :java-object/repr .. :java-object/jackson ..}` and reconstructed on read. Two loud failures by design:

- **Write**: value that Jackson can't round-trip either (e.g. an atom) → throws `:dbos.serializer/unserializable`, failing the step/workflow. Fix the step to return serializable data.
- **Read**: boxed class missing/changed on the reading JVM → throws `:dbos.serializer/reconstruct-failed`.

For types that must survive with fidelity, write a real transit handler instead of leaning on the box.

### Bringing your own serializer

`:serializer` takes any `dev.dbos.transact.json.DBOSSerializer` (5 methods: `name`, `serialize`/`deserialize`, `serializeThrowable`/`deserializeThrowable`). Rules: `serialize` must return a **String** (binary codecs like Nippy must base64-encode); `name` is frozen once data exists; instance and every client on the same rows must match.

## Testing workflows

### Unit tests: redef the execution seams

`run-step` expands to `execute-step`, `do-step!` to `execute-do-step!` — both are deliberate **redef seams**. Redefine them to run step bodies inline without a live DBOS:

```clojure
(deftest my-workflow-test
  (with-redefs [dbos.core/execute-step     (fn [_dbos _step thunk] (thunk))
                dbos.core/execute-do-step! (fn [_dbos _step thunk] (thunk) nil)]
    (is (= {:success true}
           (sync-user {:db fake-db :api fake-api} ::dbos {:user-id "u1"})))))
```

Pass any placeholder (e.g. `::dbos`) as the instance — the seams never touch it. Capture step names in an atom to assert on step structure. Note `execute-do-step!` returns `nil` — mirror that in stubs.

### Integration tests: real Postgres

The library's own suite runs against a throwaway Postgres (docker compose). Pattern: `create` + `launch!` in a fixture, run workflows with stable ids, assert on results and on `dbos.query/list-workflow-steps` / `workflow-tree`, `shutdown!` in teardown. Recovery/replay behavior can only be observed against a real database.

## clj-kondo linting

The jar ships a clj-kondo config that mechanically enforces the step rules. Import once:

```bash
mkdir -p .clj-kondo
clj-kondo --lint "$(clojure -Spath)" --dependencies --copy-configs
```

Config lands in `.clj-kondo/imports/com.shipclojure/dbos-clj/` (gitignore `.clj-kondo/imports/`; re-run on dependency bumps).

| Linter | Level | Catches |
|--------|-------|---------|
| `:dbos-clj/invalid-step` | error | step spec that can never be valid (keyword, nil, number, vector), blank name, map missing `:name` |
| `:dbos-clj/invalid-step-option` | warning | unknown option keys (e.g. `:max-attemps` typo — otherwise **silently ignored**), wrong-typed values |
| `:dbos-clj/empty-step-body` | warning | step with no body (recorded as run, does nothing) |
| `:dbos-clj/nested-step` | error | a step inside another step's body |
| `:dbos-clj/step-body-violation` | error | `start-workflow!`, `set-event!`, `workflow-sleep`, `get-event` inside a step body |

Tune any of them in `.clj-kondo/config.edn`: `{:linters {:dbos-clj/empty-step-body {:level :off}}}`.

## Logging with trove

`dbos-clj` logs step activity through [trove](https://github.com/taoensso/trove), a tiny logging facade. Point it at your backend at app startup:

```clojure
(require '[taoensso.trove :as trove]
         '[taoensso.trove.telemere :as trove-telemere])
(trove/set-log-fn! (trove-telemere/get-log-fn))
;; backends: .telemere .timbre .slf4j .mulog .tools-logging .console
```

- `dbos-clj` binds `trove/*ctx*` to `{:workflow/step "step-name"}` around every step body, so `trove/log!` calls inside steps carry the step name for free.
- To tag *native* backend calls too (bare `t/log!`, `μ/log`, MDC layouts), enable the context bridge: `(trove-telemere/get-log-fn {:bridge-ctx? true})`. Off by default; supported by Telemere, Timbre, μ/log, SLF4J.
- DBOS's internal Java logs go through SLF4J — add an SLF4J provider (e.g. `com.taoensso/telemere-slf4j`, logback) or they're dropped with "No SLF4J providers were found".
- Don't wrap log calls in steps: on replay a wrapped log won't re-fire.

## Storage schema & debugging

DBOS keeps everything in its own schema (default `dbos`, override `:schema`). Two tables matter:

- `dbos.workflow_status` — one row per workflow instance: id, status, name, class, executor, app version, queue, serialized input/output.
- `dbos.operation_outputs` — one row per step, keyed by `(workflow_uuid, function_id)`, with the serialized step output.

```sql
select workflow_uuid, status, name, class_name, output, serialization
from dbos.workflow_status order by created_at desc;

select workflow_uuid, function_name, output
from dbos.operation_outputs order by function_id;
```

Transit `json-verbose` payloads are human-readable: `{:user-id "john-123"}` stores as `{"~:user-id":"john-123"}` (`~:` = transit keyword tag; scalars stay plain).

From the REPL, prefer `dbos.query/workflow-tree` over raw SQL — it hands back the whole execution (statuses, steps, children expanded recursively) as one Clojure value with inputs/outputs already deserialized.

Workflow statuses: `PENDING`, `ENQUEUED`, `SUCCESS`, `ERROR`, `CANCELLED`, `MAX_RECOVERY_ATTEMPTS_EXCEEDED` (a repeatedly-crashing workflow is parked here once `:workflow/max-recovery-attempts` is exceeded). Use `dbos.constants` instead of hardcoding strings.
