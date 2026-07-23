# dbos-clj

A small wrapper over [dbos-transact-java](https://github.com/dbos-inc/dbos-transact-java) to support durable workflows backed by PostgreSQL or CockroachDB in Clojure.

## Why use it?

If you need durable execution & *resumable workflows* (read a great write-up on it here: [Demystifying Determinism in Durable Execution](https://jack-vanlightly.com/blog/2025/11/24/demystifying-determinism-in-durable-execution)) - DBOS is a much more lightweight alternative to something like [Temporal](https://temporal.io/) where you need to deploy a separate service just to start using it.
With DBOS, all you need is a PostgreSQL DB and you can start using it.

The Java version of DBOS has some weird kinks when it comes to interop with clojure that you can only discover through usage. That experience is boiled down to this library. It brings both functionality & ergonomics.

## Instalation

Deps:
```clojure
com.shipclojure/dbos-clj {:mvn/version "0.1.0"}
```

Or lein:

```clojure
[com.shipclojure/dbos-clj "0.1.0"]
```


## Getting started

You should read and familiarise yourself with the official [DBOS docs](https://docs.dbos.dev/).

```clojure
(require '[dbos.core :as dbos])

;; Your java.sql.Datasource DB
(def db (initialise-psql-conn!))

(defn sync-db-with-remote
  [dbos {:keys [user-id]}]
  ;; Use run-step when you need the result of your step
  (let [result (dbos/run-step dbos "fetch from db"
                 (sql-fetch db user-id))]
    ;; If you don't need the result of the operation, prefer using `do-step!` as
    ;; it only perists that the step ran, NOT the result. Useful to avoid large
    ;; data chunks being persisted for no reason.
    (dbos/do-step! dbos "sync with remote"
      (api! :post "/remote" {:body result}))

    ;; final result of the workflow
    {:success true}))

(def wf-definition {:workflow/key :workflow/sync-db-with-remote
                    :workflow/fn sync-db-with-remote})


(def dbos-instance
  (dbos/create
   {:config {:datasource db ;; Your java.sql.Datasource DB (HickariCP etc.)
             :app-name "My DBOS clojure app"
             :app-version "1.0.0" ;; See App Version section in the README
             :executor-id "my-clj-dbos-executor" ;; ID of the executor that runs the workflows
             ;; DB schema where DBOS creates its internaltables - optional, defaults to `dbos`
             :schema "dbos"}
    :workflows [wf-definition]}))

;; Launch the dbos instance - creates tables, starts checking for work to be
;; done
(dbos/launch! dbos-instance)

;; Start a workflow - This will run on an executor thread
(def handle (dbos/start-workflow!
             dbos-instance ;; instance
             :workflow/sync-db-with-remote ;; workflow name
             "sync-user-john-to-remote" ;;workflow instance id
             {:user-id "john-123"} ;; workflow input
             ))

;; do some other work

;; wait for result on the original thread
@handle ;; => {:success true}
```

## Configuration & lifecycle

The library holds no global state - *you* own the instance. `create` builds the `DBOSConfig`, registers your queues and workflows, and hands back the raw `DBOS` instance, but it does **not** launch. You launch when you're ready and shut down on the way out, so it drops cleanly into whatever component system you use (Integrant, mount, a bare atom):

```clojure
(def dbos-instance (dbos/create {:config {...} :queues [...] :workflows [...]}))
(dbos/launch! dbos-instance)     ; creates/migrates tables, starts polling for work
;; ... app runs ...
(dbos/shutdown! dbos-instance)   ; drains in-flight workflows, stops the executor
```

Queues and workflows can only be registered **before** launch - that's exactly why `create` (register) and `launch!` (go live) are separate steps.

### Config keys

`:datasource` and `:app-name` are the only things you really need; everything else is optional:

- **Database** - either `:datasource` (a `javax.sql.DataSource` like a HikariCP pool), or a JDBC url DBOS opens itself via `:database-url` + `:db-user` + `:db-password`. `:migrate?` lets DBOS create/upgrade its own system tables (needs DDL rights).
- **Identity** - `:app-name`, `:app-version` (see [App version](#app-version)), `:executor-id` (defaults to a generated id), `:schema` (where DBOS puts its tables, defaults to `dbos`).
- **Serializer** - `:serializer`, a `DBOSSerializer`; defaults to the Transit one (see [Serialization](#serialization)).
- **Queues** - `:listen-queues`, a seq of queue names *this* executor should consume from (see below).
- **Admin HTTP server** - `:admin-server?` to enable it, `:admin-server-port` for the port.
- **Other knobs** - `:scheduler-polling-interval` (a `java.time.Duration`), `:use-listen-notify?` (Postgres LISTEN/NOTIFY for queue wakeups, on by default), `:enable-patching?`.
- **Conductor** - `:conductor {:domain .. :api-key .. :executor-metadata {..}}` for DBOS Conductor.

### Queues

A queue routes work to a pool of workers. Build one with the `Queue` builders (size the worker pool, rate-limit, ...) and hand it to `create` under `:queues` - it has to exist before launch:

```clojure
(import '(dev.dbos.transact.workflow Queue))

(def dbos-instance
  (dbos/create
   {:config    {:datasource db :app-name "my-app"
                :listen-queues ["my-queue"]}              ; consume work off "my-queue"
    :queues    [(-> (Queue. "my-queue")
                    (.withWorkerConcurrency (int 8)))]    ; up to 8 concurrent per executor
    :workflows [wf-definition]}))
```

`:queues` and `:listen-queues` are independent: `:queues` *registers* each queue and its config (concurrency, rate limits), while `:listen-queues` controls which registered queues this executor actually *pulls work from*. That split is what lets you run separate executors off one codebase - e.g. an API executor that registers a queue and enqueues onto it but doesn't listen, and a worker executor that listens and runs the work.

## Workflow dispatch

There are two ways to kick off a workflow: `start-workflow!` runs it on *this* process' executor, and `enqueue-workflow!` drops it on a queue for some *other* process to pick up (see [DBOS Client](#dbos-client)).

Both take the workflow **id-or-opts** as their third argument, and you can pass it three different ways:

```clojure
(import '(dev.dbos.transact StartWorkflowOptions))
(import '(java.time Duration))

;; 1. a bare string - just the workflow instance id
(dbos/start-workflow! dbos-instance :workflow/sync-db-with-remote
                      "sync-user-john-to-remote"
                      {:user-id "john-123"})

;; 2. an options map - every key is optional, but you almost always want an id
(dbos/start-workflow! dbos-instance :workflow/sync-db-with-remote
                      {:workflow/id "sync-user-john-to-remote"
                       :workflow/queue "my-queue"          ; a name string OR a Queue instance
                       :workflow/timeout (Duration/ofMinutes 5)
                       :workflow/deduplication-id "sync-john-once"
                       :workflow/priority 10               ; lower runs first
                       :workflow/delay (Duration/ofSeconds 30)
                       :workflow/app-version "1.0.0"       ; or :latest - see App Version
                       :workflow/queue-partition-key "tenant-42"}
                      {:user-id "john-123"})

;; 3. an already-built StartWorkflowOptions, for a knob the map doesn't model
;;    (auth, attributes, ...). It's passed straight through, untouched.
(dbos/start-workflow! dbos-instance :workflow/sync-db-with-remote
                      (-> (StartWorkflowOptions.)
                          (.withWorkflowId "sync-user-john-to-remote")
                          (.withAuthenticatedUser "john"))
                      {:user-id "john-123"})
```

The **workflow instance id** is your idempotency key: start the same id twice and DBOS replays the recorded run instead of executing it again. It is *not* folded into your input map - read it inside the body with `(dbos/workflow-id)`, or outside from the handle with `(.workflowId handle)`.

Enqueuing looks the same, but goes through a `DBOSClient` and **requires a queue** (that's how a worker finds the work - there is no default queue):

```clojure
(require '[dbos.client :as client])

(client/enqueue-workflow! a-client :workflow/sync-db-with-remote
                          {:workflow/id "sync-user-john-to-remote"
                           :workflow/queue "my-queue"}    ; required
                          {:user-id "john-123"})
```

### App version

Every workflow row records the `:app-version` that started it, and by default a workflow only runs on an executor whose version matches. That's usually what you want (a workflow keeps running the code it started on), but it bites when your enqueue side and your worker are on different deploys - the work can sit unclaimed. Pin it explicitly when you need to:

```clojure
;; pin to a concrete version
{:workflow/app-version "1.0.0"}

;; or resolve the latest registered version at dispatch time
{:workflow/app-version :latest}
```

If you make changes to a workflow, you should bump the `app-version` with which your executors start, because you don't want executors on version `1.1.0` to pick up up workflows dispatched on `1.0.0`.

By default, `dbos-transact-java` will compute the `app-version` as a SHA from the bytecode of the classes defining the workflows. This is dynamic in Clojure so the app version computes a new SHA on each dbos instance creation. Therefore, we need to maintain an app version manually.

### Child workflows

Inside a workflow body you can fan out to **child workflows** with `start-workflow!` - the same fn used to dispatch top-level workflows. Called from within a workflow body, DBOS records the parent/child link via ambient context. Call it from the body only - never from inside a step - and start the children in a deterministic order (`mapv`, never `pmap`/`future`), because DBOS keys the parent/child link on call order for replay:

```clojure
(defn parent [dbos {:keys [ids]}]
  (let [handles (mapv (fn [id]
                        (dbos/start-workflow!
                         dbos :workflow/sync-db-with-remote
                         {:workflow/id (str (dbos/workflow-id) "|" id)}
                         {:user-id id}))
                      ids)]
    ;; start them all first, then await - that's where the parallelism comes from
    {:results (mapv deref handles)}))
```

### Scheduled workflows

A workflow runs on a cron schedule when its definition carries a `:workflow/schedule`. The fn is otherwise ordinary, except DBOS invokes it on each tick with `{:scheduled/at <Instant> :schedule/context <ctx>}` as the `input` - so destructure `:scheduled/at` for the fire time:

```clojure
(defn nightly-cleanup [deps dbos {:scheduled/keys [at]}]
  (dbos/run-step dbos "sweep-expired"
    (delete-expired! (:db deps) at))
  {:success true})

(def wf-definition
  {:workflow/key :workflow/nightly-cleanup
   :workflow/fn (partial nightly-cleanup {:db db})
   :workflow/schedule {:cron "0 0 3 * * *"}})   ; every day at 03:00:00
```

The cron string is the **6-field** form `second minute hour day-of-month month day-of-week` (e.g. `"*/2 * * * * *"` fires every 2 seconds), so it includes seconds - not the usual 5-field crontab.

Registering the workflow (via `create`) is *not* enough to make it fire - you also have to install the schedule row, **after** launch, with `apply-schedules!`. Pass it the same definitions; it's a no-op for any that don't carry a `:workflow/schedule`:

```clojure
(def dbos-instance
  (dbos/create {:config {...} :workflows [wf-definition]}))

(dbos/launch! dbos-instance)
(dbos/apply-schedules! dbos-instance [wf-definition])   ; after launch!
```

A schedule can optionally target a queue with `{:cron "..." :queue "my-queue"}`. Without a queue it fires on the registering executor directly; with one, run `apply-schedules!` on the executor(s) that listen on that queue so the ticks fan out to your worker pool.

## Writing workflows

A workflow is a plain Clojure fn. Its shape is `[dbos input]` (or `[deps dbos input]` when you close deps over it with `partial` at registration). `dbos` is the live instance, `input` is a single serializable map - and `input` is the *only* thing persisted for recovery, so it can't carry a db pool or an API client. Close those over the fn instead:

```clojure
{:workflow/key :workflow/sync-db-with-remote
 :workflow/fn (partial sync-db-with-remote {:db db :api api-client})}
```

Names are effectively **frozen**: the keyword's name and namespace become the `workflowName`/`className` DBOS stores in the DB and matches on recovery. Renaming a workflow orphans its in-flight instances.

A workflow definition is just a map. Its keys:

- `:workflow/key` (required) - a **namespaced keyword**; its name/namespace become the DBOS `workflowName`/`className` (the frozen identity above).
- `:workflow/fn` (required) - the `[dbos input]` fn (or `[deps dbos input]` closed over `deps`).
- `:workflow/max-recovery-attempts` (optional int) - cap on how many times DBOS retries a workflow that keeps crashing before it's parked as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`. Leave it off for the DBOS default.
- `:workflow/schedule` (optional) - `{:cron "..."}` (with an optional `:queue`) to run it on a cron; see [Scheduled workflows](#scheduled-workflows).

```clojure
{:workflow/key :workflow/sync-db-with-remote
 :workflow/fn (partial sync-db-with-remote {:db db :api api-client})
 :workflow/max-recovery-attempts 5}
```

### What should you wrap in a dbos step

A step's contract is *"executed at least once, never re-run after it completes."* On replay, a finished `run-step` returns its **recorded** value without executing the body again. That single guarantee decides what belongs in a step:

- **Wrap anything non-deterministic** the workflow later depends on - random ids, timestamps, external API calls, DB reads. Recovery replays the recorded value, so every run takes the same branches. Code *between* steps must be deterministic; it re-runs from scratch on recovery.

- **`run-step` persists the return value; `do-step!` doesn't.** Use `run-step` when you need the result later (keep it small - it's serialized). Use `do-step!` for side-effects whose result you don't need (a DB write, firing a notification). `do-step!` just persists the fact that the step ran, so it can be skipped on later resume.
- **Logging is _not_ a step.** Wrap a log in a step and it fires exactly once, ever - skipped on every retry and crash recovery, which is the opposite of what a log is for. Log **bare, between steps**.
- **Split independently-retriable work into separate steps.** A step retries as a whole, never partially. Keep "call a rate-limited API" and "write the result to the DB" as two steps - fused, a failed DB write forces the expensive fetch to redo too.
- `workflow-sleep` is durable - the wake-up time is persisted, not the thread, so it survives restarts.
- **Never start a child workflow from inside a step** - do it from the body.

```clojure
(defn sync-db-with-remote [{:keys [db api]} dbos {:keys [user-id]}]
  (let [row (dbos/run-step dbos "fetch from db"     ; DB read -> step
              (sql-fetch db user-id))]
    (t/log! :sync/fetched {:user-id user-id})       ; bare log, NOT a step
    (dbos/do-step! dbos "sync with remote"          ; side-effect -> do-step!
      (api! :post "/remote" {:body row}))
    {:success true}))                                ; deterministic result
```

#### Step retries

Both step macros take a name string **or** an options map to configure DBOS retries. A bare name means no retry (`:max-attempts` 1); a map opts in:

```clojure
(dbos/run-step dbos {:name "fetch-user" :max-attempts 3
                     :retry-interval (java.time.Duration/ofSeconds 2)
                     :backoff-rate 2.0}
  (api/get-user id))
```

Map keys: `:name` (required), `:max-attempts`, `:retry-interval` (a `Duration`), `:backoff-rate` (double), `:retry?` (predicate fn of `Throwable` -> boolean). A pre-built `StepOptions` is also accepted.

### The step macros

`dbos-clj` exposes some quality of life macros.  The [Official DBOS clojure getting started example](https://github.com/dbos-inc/dbos-demo-apps/blob/main/clojure/dbos-starter/src/dbos_starter/core.clj) shows usage like:

```clojure
(defn- step-one []
  (Thread/sleep 5000)
  (log/infof "Workflow %s step 1 completed!" (DBOS/workflowId)))

(defn- step-two []
  (Thread/sleep 5000)
  (log/infof "Workflow %s step 2 completed!" (DBOS/workflowId)))

(defn- step-three []
  (Thread/sleep 5000)
  (log/infof "Workflow %s step 3 completed!" (DBOS/workflowId)))

(defn example-workflow [^DBOS dbos]
  (.runStep dbos step-one "stepOne")
  (.setEvent dbos steps-event (Integer/valueOf 1))
  (.runStep dbos step-two "stepTwo")
  (.setEvent dbos steps-event (Integer/valueOf 2))
  (.runStep dbos step-three "stepThree")
  (.setEvent dbos steps-event (Integer/valueOf 3)))
```

with `dbos-clj` running steps can use the macros that inline the function definitions:

```clojure
 ;; Much more concise
(defn example-workflow [^DBOS dbos]
 (do-step! dbos "stepOne"
  (Thread/sleep 5000)
  (log/infof "Workflow %s step 1 completed!" (DBOS/workflowId)))

 (do-step! dbos "stepTwo"
  (Thread/sleep 5000)
  (log/infof "Workflow %s step 2 completed!" (DBOS/workflowId))

 (do-step! dbos "stepThree"
  (Thread/sleep 5000)
  (log/infof "Workflow %s step 3 completed!" (DBOS/workflowId)))

```

## Storage

DBOS keeps everything in its own schema (defaults to `dbos`, override with `:schema`). Two tables do the heavy lifting:

- `dbos.workflow_status` - one row per workflow instance: its id, status, name, class, executor, app version, queue, and the serialized input/output.
- `dbos.operation_outputs` - one row per step (`run-step`/`do-step!`), keyed by `(workflow_uuid, function_id)`, with the step's serialized output.

On recovery DBOS reads these back: a workflow resumes from the last step that completed, replaying the recorded outputs instead of re-executing them.

```sql
select workflow_uuid, status, name, class_name, output, serialization
from dbos.workflow_status order by created_at desc;

select workflow_uuid, function_name, output
from dbos.operation_outputs order by function_id;
```

Because `dbos-clj` swaps in a Transit `json-verbose` serializer, the payloads are **human-readable** and round-trip as real Clojure data. A workflow started with input `{:user-id "john-123"}` that returns `{:success true}` stores:

| column          | value                      |
|-----------------|----------------------------|
| `inputs`        | `{"~:user-id":"john-123"}` |
| `output`        | `{"~:success":true}`       |
| `serialization` | `transit_json_verbose`     |

The `~:` prefix is Transit's tag for a keyword; scalars stay plain (`[1,2,3]` is just `[1,2,3]`). The `serialization` column records the format name per row - treat `transit_json_verbose` as frozen once you have live workflow data, since it's how DBOS knows which reader to use.


## Serialization

Workflow inputs, outputs, errors  and results/errros from each step are serialized in the database. In case of a crash, the workflow is retried from the last successfull persisted step.

### Why does `dbos-clj` bring its own serialization. What's wrong with the default in DBOS?
The dbos-transact-java, uses [jackson-databind](https://github.com/FasterXML/jackson-databind) to serialize/deserialize objects. The default cannot be used in clojure because when deserializing persistent data structures, jackson tries to mutate them in place causing them to throw. Because of this, [Transit](https://github.com/cognitect/transit-clj) is used as the serializer, defaulting to DBOS's default serializer when transit doesn't have handlers for that particular object.

You can also provide your own serializer, but ensure it behaves well with persistent data structure deserialization.

### Injecting your own transit handlers

Out of the box the serializer bundles **no** custom handlers, so plain Clojure data (keywords, UUIDs, collections) round-trips fine, but types like `java.time` values don't get first-class fidelity. Pass your app-wide handlers to `transit-serializer` and wire the result in through `:serializer`:

```clojure
(require '[dbos.serializer :as serializer])

(def ser
  (serializer/transit-serializer
   {:write-handlers my.app.transit/write-handlers
    :read-handlers  my.app.transit/read-handlers}))

(dbos/create {:config {:app-name "my-app" :datasource db :serializer ser} ...})
```

### Unhandled types (the `java-object` box)

When a value has no transit handler, the serializer doesn't give up - it boxes it as a `java-object`: `{:java-object/class .. :java-object/repr .. :java-object/jackson ..}`, with a `:dbos.serializer/java-object-boxed` warning log, and reconstructs the real object on read. Two cases fail loudly instead, both by design (a durable system shouldn't silently persist or resurrect bad state):

- **On write** - if the value can't round-trip through DBOS's own Jackson serializer either (e.g. an atom), serialization **throws** `:dbos.serializer/unserializable` and fails the step/workflow. Fix it to return serializable data.
- **On read** - if a boxed value's class is missing or has changed shape on the JVM doing the read, it **throws** `:dbos.serializer/reconstruct-failed`.

The format name recorded per row is the frozen `transit_json_verbose` - it's how DBOS knows which reader to use, so don't change it once you have live data. For types that must survive with full fidelity, a real transit handler beats leaning on the java-object box.


## DBOS Client

If you run a DBOS executor outside your main app process but you still want to dispatch workflows to it, use the client. A common setup: a lightweight worker process registers and runs the workflows, while your web app just enqueues them.

```clojure
(require '[dbos.client :as client])

(def a-client
  (client/create-client
   {:datasource db                         ; a javax.sql.DataSource, OR
    ;; :database-url "jdbc:postgresql://..." :db-user "..." :db-password "..."
    :schema "dbos"                          ; optional, match your executor's schema
    :serializer my-serializer}))            ; optional, but see below

;; enqueue - the worker listening on :workflow/queue picks it up
(client/enqueue-workflow! a-client :workflow/sync-db-with-remote
                          {:workflow/id "sync-user-john" :workflow/queue "my-queue"}
                          {:user-id "john-123"})

;; the client holds no running workflows, so just close it on shutdown
(.close a-client)
```

Things to know:

- **Point it at the same database** your executor uses - the client dispatches by writing rows into DBOS's tables, it doesn't call the executor directly.
- **Use the same serializer** you configured on the instance, so the input you enqueue deserializes correctly on the worker side. If you don't pass one, it defaults to the same Transit serializer the instance uses by default.
- **A queue is required** when enqueuing (see [Workflow dispatch](#workflow-dispatch)).
- There's **no client-side registry**: the `(workflowName, className)` pair is derived from the workflow keyword, so a typo'd keyword doesn't fail here - it surfaces as a durable `NOT_FOUND` when the worker tries to run it.
- The client can also do read-side work: `retrieve-workflow` gets a derefable handle to an already-enqueued workflow by id, and the querying/event fns below accept a client too.

```clojure
(let [handle (client/retrieve-workflow a-client "sync-user-john")]
  @handle)   ; block for the result from another process
```

## Querying workflows

The read-side lives in `dbos.query` and works the same on a DBOS instance or a `DBOSClient` - pass whichever you have. Everything takes Clojure maps in and hands Clojure maps back.

```clojure
(require '[dbos.query :as query]
         '[dbos.constants :as const])

;; a single workflow's status by id, or nil if it doesn't exist
(query/get-workflow-status dbos-instance "sync-user-john")
;; => {:workflow-id "sync-user-john" :status "SUCCESS" :workflow-name "..." ...}

;; list with filters - returns a vector of status maps
(query/list-workflows dbos-instance
                      {:workflow-name "sync-db-with-remote"
                       :statuses [const/status-pending const/status-enqueued]
                       :workflow-id-prefix "sync-"
                       :limit 50
                       :sort-desc? true})

;; the recorded steps of one workflow, in execution order
(query/list-workflow-steps dbos-instance "sync-user-john")
;; => [{:function-id 0 :function-name "fetch from db" :output {...} ...} ...]
```

Status strings and handy sets (`terminal-statuses`, `in-progress-statuses`, ...) live in `dbos.constants`, which is pure `.cljc` data - safe to share with a ClojureScript UI.

## Events

Events are a durable key/value channel on a running workflow - the workflow publishes progress under a key, and anyone (a request handler, another process) reads the latest value back. Reporting progress to a UI is the classic use.

```clojure
;; INSIDE the workflow body - publish. Last write wins; it's durable and
;; idempotent under replay, so it doesn't need a step wrapper. Body-only:
;; set-event! throws from inside a step or outside a workflow.
(defn ingest [dbos {:keys [items]}]
  (dbos/set-event! dbos :progress {:done 0 :total (count items)})
  ;; ... do work ...
  (dbos/set-event! dbos :progress {:done (count items) :total (count items)})
  {:success true})

;; OUTSIDE the workflow - read the latest value (nil if nothing published yet).
;; Non-blocking; works with a DBOS instance or a DBOSClient.
(dbos/get-event dbos-instance "ingest-42" :progress)
;; => {:done 3 :total 10}
```
