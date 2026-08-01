# dbos-clj API Reference

Contents:
- [Config keys (`dbos/create` `:config`)](#config-keys)
- [Queues](#queues)
- [start-workflow! options](#start-workflow-options)
- [DBOS Client (`dbos.client`)](#dbos-client)
- [Querying (`dbos.query`)](#querying)
- [Constants (`dbos.constants`)](#constants)
- [Cancel / resume](#cancel--resume)
- [App versions](#app-versions)

## Config keys

`(dbos/create {:config {...} :queues [...] :workflows [...]})` — `:app-name` plus a database source are required, everything else optional:

| Key | Meaning |
|-----|---------|
| `:datasource` | any `javax.sql.DataSource` (HikariCP pool etc.) |
| `:database-url` `:db-user` `:db-password` | JDBC url alternative to `:datasource` |
| `:migrate?` | let DBOS create/upgrade its system schema (needs DDL rights) |
| `:app-name` | required application name |
| `:app-version` | set manually in Clojure (bytecode SHA default is useless — dynamic classes) |
| `:executor-id` | id of this executor (defaults to generated) |
| `:schema` | schema for DBOS system tables (default `dbos`) |
| `:serializer` | a `DBOSSerializer`; defaults to transit (see operations.md) |
| `:listen-queues` | seq of queue names **this** executor consumes from |
| `:admin-server?` / `:admin-server-port` | DBOS admin HTTP server |
| `:scheduler-polling-interval` | `java.time.Duration` for the cron scheduler poll |
| `:use-listen-notify?` | Postgres LISTEN/NOTIFY queue wakeups (default on) |
| `:enable-patching?` | DBOS workflow patching support |
| `:conductor` | `{:domain .. :api-key .. :executor-metadata {..}}` for DBOS Conductor |

## Queues

A queue routes work to a pool of workers. Register before launch under `:queues`:

```clojure
(import '(dev.dbos.transact.workflow Queue))

(dbos/create
 {:config    {:datasource db
              :app-name "my-app"
              :listen-queues ["my-queue"]}            ; consume off "my-queue"
  :queues    [(-> (Queue. "my-queue")
                  (.withWorkerConcurrency (int 8)))]  ; 8 concurrent per executor
  :workflows [...]})
```

`Queue` builder methods (it's an immutable record): `.withConcurrency` (global cap across executors), `.withWorkerConcurrency` (per-executor cap), `.withRateLimit` (`(int limit, Duration period)`), `.withPriorityEnabled`, `.withPartitioningEnabled`, `.withPollingInterval`.

**`:queues` vs `:listen-queues`**: `:queues` *registers* a queue and its config; `:listen-queues` controls which registered queues this executor *pulls from*. The split enables one codebase, multiple roles: an API executor registers + enqueues but doesn't listen; a worker executor listens and runs the work.

## start-workflow! options

`(dbos/start-workflow! instance wf-key id-or-opts input)` — `id-or-opts` takes three forms:

```clojure
;; 1. bare string = workflow instance id
"sync-user-john"

;; 2. options map (every key optional, but you almost always want an id)
{:workflow/id "sync-user-john"
 :workflow/queue "my-queue"                  ; name string OR Queue instance
 :workflow/timeout (Duration/ofMinutes 5)
 :workflow/deduplication-id "sync-john-once" ; separate from :workflow/id
 :workflow/priority 10                       ; lower runs first
 :workflow/delay (Duration/ofSeconds 30)
 :workflow/app-version "1.0.0"               ; or :latest (resolved at dispatch)
 :workflow/deadline some-instant
 :workflow/queue-partition-key "tenant-42"}

;; 3. pre-built StartWorkflowOptions, for knobs the map doesn't model
;;    (auth, attributes...). Passed through VERBATIM: :latest resolution and
;;    blank-string guards do NOT apply.
(-> (StartWorkflowOptions.)
    (.withWorkflowId "sync-user-john")
    (.withAuthenticatedUser "john"))
```

Returns a deref-able `WorkflowHandle`: `@handle` blocks for the (deserialized) result; `(.workflowId handle)`, `(.getStatus handle)` also available. Blank string option values are treated as absent (the Java option ctors throw on empty strings).

Throws `Workflow not registered` ex-info if `wf-key` was never registered on this instance.

## DBOS Client

`dbos.client` — dispatch/read from a process that doesn't run an executor (e.g. web app enqueues, worker executes). The client writes rows to DBOS's tables; it never talks to the executor directly.

```clojure
(require '[dbos.client :as client])

(def a-client
  (client/create-client
   {:datasource db              ; OR :database-url + :db-user + :db-password
    :schema "dbos"              ; match the executor's schema
    :serializer my-serializer})) ; MUST match the executor's serializer

(client/enqueue-workflow! a-client :myapp/sync-user
                          {:workflow/id "sync-user-john"
                           :workflow/queue "my-queue"}   ; queue REQUIRED
                          {:user-id "john-123"})

(def handle (client/retrieve-workflow a-client "sync-user-john"))
@handle       ; block for result from another process

(.close a-client)  ; caller owns lifecycle
```

Things to know:

- **A queue is required** when enqueuing (map/string forms throw without `:workflow/queue`); a pre-built `DBOSClient$EnqueueOptions` encodes its own queue.
- **No client-side registry**: `(workflowName, className)` derive from the keyword, so a typo'd `wf-key` doesn't fail at enqueue — it surfaces as a durable `NOT_FOUND` on the worker.
- Same database, same schema, same serializer as the executor — or inputs won't deserialize.
- The query fns, `get-event`, `cancel-workflow!` and `resume-workflow!` all accept a client too.

## Querying

`dbos.query` works identically on a DBOS instance or a `DBOSClient`. Maps in, maps out; inputs/outputs come back deserialized as real Clojure data.

```clojure
(require '[dbos.query :as query] '[dbos.constants :as const])

;; single status map by id, or nil
(query/get-workflow-status x "sync-user-john")
;; => {:workflow-id ".." :status "SUCCESS" :workflow-name ".." :class-name ".."
;;     :executor-id ".." :created-at .. :updated-at .. :app-version ".."
;;     :recovery-attempts .. :queue-name .. :input [..] :output .. :error ..
;;     :parent-workflow-id .. :priority .. :timeout-ms ..}

;; filtered list -> vector of status maps
(query/list-workflows x
  {:workflow-name "sync-user"          ; also :workflow-ids, :status (single),
   :statuses [const/status-pending     ; :queue-name, :executor-ids,
              const/status-enqueued]   ; :start-time/:end-time (OffsetDateTime),
   :workflow-id-prefix "sync-"         ; :offset, :load-input?, :load-output?
   :limit 50
   :sort-desc? true})

;; recorded steps in execution order
(query/list-workflow-steps x "sync-user-john")
;; => [{:function-id 0 :function-name "fetch-from-db" :output {..} :error nil
;;      :child-workflow-id nil :started-at .. :completed-at .. :serialization ".."} ..]

;; whole execution tree - parent status + :steps, children expanded in place
(query/workflow-tree x "sync-all-users" {:max-depth 10})
```

`workflow-tree` notes: awaiting a child records a second `DBOS.getResult` step pointing at the same child (expanded once, at the starting step); `workflow-sleep` shows as a `DBOS.sleep` step whose output is the wake-up time. One query per workflow in the tree — debugging tool, not hot-path.

## Constants

`dbos.constants` is pure-data `.cljc` (shareable with a CLJS UI): `status-pending`, `status-enqueued`, `status-success`, `status-error`, `status-cancelled`, `status-max-recovery-attempts-exceeded`, plus sets `in-progress-statuses`, `error-statuses`, `terminal-statuses`, `all-statuses`.

## Cancel / resume

Work on either a DBOS instance or a `DBOSClient`:

```clojure
(dbos/cancel-workflow! x "wf-id")     ; marks CANCELLED, stops further steps; returns id
@(dbos/resume-workflow! x "wf-id")    ; resumes from where it left off; derefable handle
```

## App versions

```clojure
(dbos/get-latest-app-version x)        ; {:version-id .. :version-name ..} or nil
(dbos/list-app-versions x)             ; vector of version maps
(dbos/set-latest-app-version! x "id")  ; pin latest, overriding DBOS's pinning
```

All accept an instance or client. `{:workflow/app-version :latest}` in dispatch options resolves via `get-latest-app-version` at dispatch time.

## Events

```clojure
;; INSIDE the workflow body only (throws in a step or outside a workflow).
;; Durable, idempotent under replay, last write wins - no step wrapper needed.
(dbos/set-event! dbos :progress {:done 3 :total 10})

;; OUTSIDE - non-blocking read of latest value (nil if none). Instance or client.
(dbos/get-event x "wf-id" :progress)
```
