---
name: dbos-clj-workflows
description: Write durable, resumable workflows in Clojure using dbos-clj (com.shipclojure/dbos-clj), a wrapper over dbos-transact-java backed by PostgreSQL. Use when writing, reviewing, or debugging DBOS workflows in Clojure - including workflow definitions, run-step/do-step! steps, start-workflow!/enqueue-workflow! dispatch, queues, scheduled (cron) workflows, child workflows/fan-out, workflow events, DBOSClient usage, transit serialization, or querying workflow status. Triggers include mentions of dbos-clj, dbos.core, dbos.client, dbos.query, run-step, do-step!, start-workflow!, enqueue-workflow!, workflow-sleep, set-event!, apply-schedules!, durable execution in Clojure, or DBOS workflows.
---

# Writing DBOS Workflows with dbos-clj

`dbos-clj` (`com.shipclojure/dbos-clj`) wraps [dbos-transact-java](https://github.com/dbos-inc/dbos-transact-java) to provide durable, crash-resumable workflows in Clojure. All you need is PostgreSQL — no separate orchestrator service (unlike Temporal).

```clojure
;; deps.edn
com.shipclojure/dbos-clj {:mvn/version "0.5.0"}
```

Namespaces: `dbos.core` (workflows, steps, lifecycle), `dbos.client` (out-of-process enqueue), `dbos.query` (read-side), `dbos.constants` (status strings, `.cljc`), `dbos.serializer` (transit serializer).

## Mental model: durable execution

DBOS persists every workflow input and every completed step result to Postgres (`dbos.workflow_status`, `dbos.operation_outputs`). On crash/restart, a workflow **replays**: completed steps return their *recorded* values without re-executing; execution resumes from the first incomplete step. Everything follows from one contract:

> A step is executed **at least once**, and **never re-run after it completes**. Code *between* steps re-runs from scratch on every replay, so it must be deterministic.

## Lifecycle

The library holds no global state — you own the instance:

```clojure
(require '[dbos.core :as dbos])

(def instance
  (dbos/create
   {:config    {:datasource db            ; javax.sql.DataSource (HikariCP etc.)
                :app-name "my-app"
                :app-version "1.0.0"}     ; maintain manually — see below
    :queues    [...]                      ; optional Queue instances
    :workflows [wf-definition ...]}))     ; definition maps

(dbos/launch! instance)                   ; creates tables, starts polling
(dbos/apply-schedules! instance defs)     ; AFTER launch, only if cron schedules
;; ... app runs ...
(dbos/shutdown! instance)                 ; drains in-flight workflows
```

Queues and workflows can only be registered **before** `launch!` — that's why `create` and `launch!` are separate. Fits Integrant/mount/component cleanly. Full config keys: see [references/api-reference.md](references/api-reference.md).

## Workflow definitions

A workflow is a plain fn of `[dbos input]`. Close dependencies over it with `partial` — `input` is the only thing persisted, so it must be serializable data (never a db pool or API client):

```clojure
(defn sync-user [{:keys [db api]} dbos {:keys [user-id]}]
  (let [row (dbos/run-step dbos "fetch-from-db"      ; DB read -> step
              (sql-fetch db user-id))]
    (dbos/do-step! dbos "push-to-remote"             ; side-effect -> do-step!
      (api! :post "/remote" {:body row}))
    {:success true}))                                ; deterministic return

(def wf-definition
  {:workflow/key :myapp/sync-user                    ; namespaced keyword (required)
   :workflow/fn (partial sync-user {:db db :api api}) ; [dbos input] after partial
   :workflow/max-recovery-attempts 5                 ; optional int
   ;; :workflow/schedule {:cron "0 0 3 * * *"}      ; optional - see patterns.md
   })
```

**Names are frozen.** The keyword's name/namespace become DBOS's `workflowName`/`className`, stored in the DB and matched on recovery. Renaming a workflow orphans its in-flight instances.

## Steps: the core rules

`run-step` persists the return value; `do-step!` only persists *that the step ran*.

```clojure
(dbos/run-step dbos "fetch-user" (api/get-user id))       ; need the result later
(dbos/do-step! dbos "send-email" (send-email! user))      ; side-effect only
```

Rules (the clj-kondo config ships linters for most of these):

1. **Wrap anything non-deterministic** the workflow later depends on: random ids, timestamps (`Instant/now`), external API calls, DB reads. Replay returns the recorded value, so every run takes the same branches.
2. **Code between steps must be deterministic** — it re-runs from scratch on recovery.
3. **`run-step` results are serialized — keep them small.** Prefer `do-step!` when the result isn't needed.
4. **Split independently-retriable work into separate steps.** A step retries as a whole: keep "call rate-limited API" and "write result to DB" as two steps.
5. **Never nest a step inside another step.**
6. **Never call `start-workflow!`, `set-event!`, `workflow-sleep`, or `get-event` inside a step body** — workflow-body only.
7. **Sleep with `(dbos/workflow-sleep dbos duration)`**, never `Thread/sleep` — the wake-up time is persisted, so it survives restarts.
8. Logging is deliberately *not* wrapped in steps (a wrapped log won't re-fire on replay).

### Step retries

A bare name string means **no retry** (`max-attempts` 1). Pass a map to opt in:

```clojure
(dbos/run-step dbos {:name "fetch-user"
                     :max-attempts 3
                     :retry-interval (java.time.Duration/ofSeconds 2)
                     :backoff-rate 2.0
                     :retry? #(instance? java.io.IOException %)}   ; optional predicate
  (api/get-user id))
```

Beware: unknown map keys are **silently dropped** (`:max-attemps` typo = no retries). Import the shipped clj-kondo config to catch this — see [references/operations.md](references/operations.md).

## Dispatching workflows

```clojure
;; id-or-opts: bare id string, options map, or pre-built StartWorkflowOptions
(def handle
  (dbos/start-workflow! instance :myapp/sync-user
                        "sync-user-john"           ; workflow instance id
                        {:user-id "john-123"}))    ; single serializable input
@handle   ; => blocks for result, e.g. {:success true}
```

- The **workflow id is the idempotency key**: starting the same id twice replays the recorded run instead of re-executing. Omit it (`nil`/`{}`) and DBOS assigns a random UUID (no dedup).
- Read the id inside the body with `(dbos/workflow-id)`, outside via `(.workflowId handle)`. It is NOT injected into `input`.
- The options-map form takes `:workflow/id`, `:workflow/queue`, `:workflow/timeout`, `:workflow/deduplication-id`, `:workflow/priority`, `:workflow/delay`, `:workflow/app-version` (string or `:latest`), `:workflow/deadline`, `:workflow/queue-partition-key` — see [references/api-reference.md](references/api-reference.md).
- To dispatch from a process that doesn't run an executor, use `dbos.client/enqueue-workflow!` (a queue is **required**) — see api-reference.md.

## App version

Every workflow row records the `:app-version` that started it; workflows only run on executors with a matching version. Java DBOS derives the version from a bytecode SHA — meaningless in Clojure (new SHA per instance) — so **always set `:app-version` manually** and bump it when workflow code changes. Pin dispatches with `{:workflow/app-version "1.0.0"}` or resolve at dispatch time with `:latest`.

## Common quick reference

| Task | Call |
|------|------|
| Value-returning step | `(dbos/run-step dbos "name" body...)` |
| Side-effect step | `(dbos/do-step! dbos "name" body...)` |
| Durable sleep | `(dbos/workflow-sleep dbos (Duration/ofSeconds 30))` |
| Current workflow id (in body) | `(dbos/workflow-id)` |
| Publish progress/event (in body) | `(dbos/set-event! dbos :progress {:done 3})` |
| Read event (outside) | `(dbos/get-event instance-or-client wf-id :progress)` |
| Start child workflow (in body) | `dbos/start-workflow!` — body only, deterministic order (`mapv`, never `pmap`) |
| Status of a workflow | `(dbos.query/get-workflow-status instance-or-client wf-id)` |
| Debug full execution tree | `(dbos.query/workflow-tree instance-or-client wf-id)` |
| Cancel / resume | `(dbos/cancel-workflow! x id)` / `@(dbos/resume-workflow! x id)` |

## Going deeper

- **[references/api-reference.md](references/api-reference.md)** — read when configuring instances or building option maps: full config keys, queues (`Queue` builders, `:listen-queues`), start/enqueue options, `dbos.client`, `dbos.query`, `dbos.constants`.
- **[references/patterns.md](references/patterns.md)** — read when implementing: child workflows and fan-out/scatter-gather, scheduled (cron) workflows, events/progress reporting, web-app + worker split via `DBOSClient`, Integrant wiring.
- **[references/operations.md](references/operations.md)** — read for: serialization (transit handlers, custom serializers, the java-object box), testing workflows with redef seams, clj-kondo lint setup, logging with trove, storage schema and debugging.
