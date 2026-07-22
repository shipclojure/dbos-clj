# dbos-clj

A small Clojure wrapper around
[DBOS Transact](https://github.com/dbos-inc/dbos-transact-java) (`dev.dbos/transact`)
for durable workflow execution on the JVM.

Logging is routed through the [Trove](https://github.com/taoensso/trove) facade,
so the library never depends on a concrete logging backend — you pick one
(μ/log, Telemere, …) and wire it in one line.


## Installation

`deps.edn`:

```clojure
com.shipclojure/dbos-clj {:git/sha "..."}
```

The library declares only three hard dependencies: `dev.dbos/transact`,
`com.taoensso/trove`, and `com.cognitect/transit-clj`.

## Namespaces

| Namespace        | Responsibility                                                                                     |
|------------------|----------------------------------------------------------------------------------------------------|
| `dbos.core`      | The whole instance-facing surface: step macros, workflow-body helpers, config builder, `create`/`launch!`/`shutdown!`, registration, `start-workflow!`, scheduled-workflow bridge, `apply-schedules!`, and the `*context-fn*` hook. |
| `dbos.serializer`| Transit `json-verbose` `DBOSSerializer` + `java-object` Jackson box. Transit handlers are caller-injected. |
| `dbos.client`    | `create-client` + `enqueue-workflow!` for out-of-process enqueuing.                                |
| `dbos.query`     | `list-workflows` / `get-workflow-status` over both a `DBOS` instance and a `DBOSClient`.           |
| `dbos.constants` | Workflow status constants + status sets.                                                           |

## Quick start

### 1. Define workflows

A workflow is a plain fn of `[dbos input]`. The `dbos` instance is closed over at
registration (it cannot be a serialized argument); `input` is the single
serializable map argument.

```clojure
(ns my.app.workflows
  (:require [dbos.core :as dbos :refer [run-step do-step!]]))

(defn greet-workflow [dbos input]
  (let [stamped (run-step dbos "stamp"
                  (assoc input :at (java.time.Instant/now)))]
    (do-step! dbos "notify"
      (send-notification! stamped))            ; side-effect only, not persisted
    (assoc stamped :status :done)))

(def definitions
  [{:workflow/name :my.app/greet
    :workflow/fn greet-workflow}])
```

Workflow definition keys:

- `:workflow/name` — a **namespaced keyword** (required). Its name/namespace
  become the DBOS `workflowName`/`className` recorded in the DB and matched on
  recovery. Treat as frozen once workflows have run.
- `:workflow/fn` — the `[dbos input]` fn (required).
- `:workflow/max-recovery-attempts` — optional integer.
- `:workflow/schedule` — optional `{:cron "..."}` (with an optional
  `:queue "..."`) for scheduled workflows (see below).

Need runtime dependencies inside a workflow? Close them over at registration
with `partial`:

```clojure
{:workflow/name :my.app/greet
 :workflow/fn (partial greet-workflow deps)}   ; fn becomes [deps dbos input]
```

### 2. Own the lifecycle

The library is stateless — you wire `create` / `launch!` / `shutdown!` into your
own state cell. `create` builds and registers but does **not** launch, so you
control when the executor goes live.

```clojure
(require '[dbos.core :as dbos])
(import '(dev.dbos.transact.workflow Queue))

(def instance
  (dbos/create
   {:config    {:app-name     "my-app"
                :datasource   my-datasource        ; a javax.sql.DataSource
                :executor-id  "my-app-executor"}   ; optional
    :queues    [(-> (Queue. "my-queue")
                    (.withWorkerConcurrency (int 8)))]
    :workflows my.app.workflows/definitions}))

(dbos/launch! instance)
;; ... later
(dbos/shutdown! instance)
```

`create` returns the raw `DBOS` instance; pass it directly to `launch!`,
`start-workflow!`, `apply-schedules!` and `shutdown!`.

`dbos-config` inputs (all but `:app-name` and a database source are optional):

- database: either `:datasource` (a `javax.sql.DataSource`) **or** `:database-url`
  + `:db-user` + `:db-password`. `:migrate?` lets DBOS create/upgrade its schema.
- `:serializer` — defaults to the transit serializer with no injected handlers.
- `:app-version`, `:executor-id`, `:schema`, `:listen-queues`, `:admin-server?`,
  `:conductor` (`{:domain .. :api-key ..}`).

### 3. Start workflows

```clojure
;; a bare workflow-id string:
(let [handle (dbos/start-workflow! instance :my.app/greet "greet-42" {:name "Ada"})]
  @handle)   ; blocks and returns the (deserialized) result

;; or an options map with namespaced keys (queue can be a name or a Queue):
(dbos/start-workflow! instance :my.app/greet
                      {:workflow/id "greet-42" :workflow/queue "my-queue"}
                      {:name "Ada"})
```

Pass a stable workflow-id for idempotent starts — re-starting the same id replays
the recorded run rather than executing again. The options map accepts
`:workflow/id`, `:workflow/queue`, `:workflow/timeout`,
`:workflow/deduplication-id`, `:workflow/priority`, and `:workflow/delay`.

### 4. Enqueue from another process

For out-of-process (fire-and-forget) execution, build a client and enqueue. A
worker listening on the queue picks it up.

```clojure
(require '[dbos.client :as client])

(def c (client/create-client {:datasource my-datasource}))

(let [handle (client/enqueue-workflow! c :my.app/greet
                                       {:workflow/id "greet-99" :workflow/queue "my-queue"}
                                       {:name "Grace"})]
  @handle)

;; the client is a stateless enqueuer — .close it whenever (or never)
(.close c)
```

### 5. Query status

`list-workflows` / `get-workflow-status` work on **either** a `DBOS` instance or
a `DBOSClient` — same call, dispatched on type.

```clojure
(require '[dbos.query :as query])

(query/get-workflow-status c "greet-99")
;; => {:workflow-id "greet-99" :status "SUCCESS" ...}

(query/list-workflows instance
                      {:workflow-id-prefix "greet-" :statuses ["SUCCESS"]})
;; => [{...} {...}]
```

Status strings and sets live in `dbos.constants` (`status-success`,
`terminal-statuses`, …).

### Scheduled workflows

Add a `:workflow/schedule`, then call `apply-schedules!` **after** launch. The
`:queue` is optional — with one, fire the schedule from the executor(s) that
listen on it; without one, the schedule fires on the registering executor
directly:

```clojure
{:workflow/name :my.app/nightly
 :workflow/fn nightly-workflow
 :workflow/schedule {:cron "0 0 * * *"}}          ; or add :queue "my-scheduled-queue"

;; after launch!:
(dbos/apply-schedules! instance my.app.workflows/definitions)
```

A scheduled workflow fn receives `{:scheduled/at <Instant> :schedule/context <ctx>}`
as its `input`.

### Child workflows

From inside a workflow body, start and await a child on the workflow's own thread
(deterministic order — never `pmap`/`future`):

```clojure
(defn parent [dbos input]
  (let [handle (dbos/start-child-workflow!
                dbos :my.app/greet
                {:workflow-id (str (dbos/workflow-id) "|child")}
                input)]
    {:child @handle}))
```

## Logging (Trove)

The library emits structured logs via `taoensso.trove/log!` under stable ids
(`:dbos/start`, `:dbos/stop`, `:dbos.workflow/step-start`,
`:dbos.workflow/registration`, `:dbos.serializer/java-object-boxed`, …). It never
picks a backend — you do, once at startup:

```clojure
;; μ/log backend:
(require '[taoensso.trove :as trove]
         '[taoensso.trove.mulog :as trove-mulog])
(trove/set-log-fn! (trove-mulog/get-log-fn))

;; Telemere backend:
(require '[taoensso.trove :as trove]
         '[taoensso.trove.telemere :as trove-telemere])
(trove/set-log-fn! (trove-telemere/get-log-fn))
```

### Contextual tagging (`set-step-context-fn!`)

Trove core has no `with-context`. To keep ambient step tagging — `:workflow/step`
riding along on **every** log emitted inside a step body, not just the step-start
log — `run-step`/`do-step!` wrap their **entire body** in a pluggable
`dbos.core/*step-context-fn*` (a fn of `[ctx-map thunk]`, no-op by default). Set
it once at startup with `set-step-context-fn!`, wrapping your backend's context
macro:

```clojure
(require '[dbos.core :as dbos])

;; μ/log:
(dbos/set-step-context-fn!
  (fn [ctx f] (com.brunobonacci.mulog/with-context ctx (f))))

;; Telemere:
(dbos/set-step-context-fn!
  (fn [ctx f] (taoensso.telemere/with-ctx+ ctx (f))))
```

So given:

```clojure
(run-step dbos "my-logging-step"
  (some-op-with-logging ...))   ; every log in here carries {:workflow/step "my-logging-step"}
```

the step-start log **and** any log emitted deep inside `some-op-with-logging`
inherit `:workflow/step "my-logging-step"`.

`*step-context-fn*` is set as a **root value**, so it is visible on every thread —
including the DBOS worker threads that execute workflow bodies. The hook runs
synchronously on the executing thread, so the context reliably covers the step
body on that same thread.

Consumers who don't wire it get the no-op default (the step name still lands in
the step-start log's `:data`).

## Serializer

Workflow inputs/outputs and step results persist as Transit `json-verbose`
(`serializer-name` is the frozen `"transit_json_verbose"`). Clojure data
(keywords, UUIDs, collections) round-trips natively; values with no transit
handler fall through to a `java-object` box (Jackson, fail-fast on unserializable
values, reconstruct on read).

The library bundles **no** transit handlers. Inject your app-wide handlers (e.g.
`java.time`) so those types get first-class fidelity and human-readable rows:

```clojure
(require '[dbos.serializer :as serializer])

(def ser
  (serializer/transit-serializer
   {:write-handlers my.app.transit/write-handlers
    :read-handlers  my.app.transit/read-handlers}))

;; pass it to the config:
(dbos/create {:config {:app-name "my-app" :datasource ds :serializer ser} ...})
```

With no handlers, the serializer still works — plain data round-trips and
`java.time` values fall through to the `java-object` box.

## Example

[`example/`](example/) is a very minimalistic Integrant + Telemere app that
uses the library the way a real consumer does (modeled on `digital-worker-poc`),
boiled down to the `dummy` and `dummy-parent` workflows. It wires workflows as
`#ig/refset :dbos/workflow` Integrant components, a HikariCP datasource, a
`:dbos/instance` component around `create`/`launch!`/`shutdown!`, and Trove →
Telemere logging. The live-DBOS integration suite boots this system, so the
library gets exercised through realistic consumer wiring. See
[`example/README.md`](example/README.md).

## Testing

```bash
./bin/kaocha                 # unit suite (no database needed)
./bin/kaocha integration     # live-DBOS suite (boots example/, needs Postgres)
```

The integration suite reads its database config from env vars:

```bash
DBOS_TEST_DATABASE_URL=jdbc:postgresql://localhost:5432/dbos_clj_test \
DBOS_TEST_DB_USER=postgres \
DBOS_TEST_DB_PASSWORD=postgres \
  ./bin/kaocha integration
```

A throwaway container works:

```bash
docker run -d --name dbos-clj-pg \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=dbos_clj_test \
  -p 5432:5432 postgres:16
```

## Building & releasing

Built with `tools.build` and deployed to Clojars via
[kaven](https://github.com/kepler16/kaven), through `build/build.clj` (the
`:build` alias). The version is derived from the latest `vMAJOR.MINOR.PATCH`
git tag.

```bash
clojure -T:build build      # -> target/dbos-clj-<version>.jar (needs a v* tag)
clojure -T:build release    # build, then deploy to Clojars
clojure -T:build clean

# override the version instead of using a git tag
clojure -T:build build :version '"0.1.0"'
```

`release` reads Clojars credentials from `CLOJARS_USERNAME` /
`CLOJARS_PASSWORD`. `bb build` / `bb release` wrap these.

## License

TBD.
