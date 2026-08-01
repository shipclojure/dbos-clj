# dbos-clj Workflow Patterns

Contents:
- [Child workflows](#child-workflows)
- [Fan-out / scatter-gather](#fan-out--scatter-gather)
- [Scheduled (cron) workflows](#scheduled-cron-workflows)
- [Progress reporting with events](#progress-reporting-with-events)
- [Web app + worker split](#web-app--worker-split)
- [Integrant wiring](#integrant-wiring)

## Child workflows

Call `start-workflow!` from *inside* a workflow body to start a child — DBOS records the parent/child link via ambient context. Two hard rules:

- **Body only, never inside a step** — a step body is skipped on replay, so the child would never restart and parent linkage is lost.
- **Deterministic start order** — `mapv`, never `pmap`/`future`. DBOS keys the parent/child link on call order for replay.

Derive child ids from the parent id for traceability and idempotency:

```clojure
(defn parent [_deps dbos input]
  (let [handle (dbos/start-workflow!
                dbos :myapp/child
                {:workflow/id (str (dbos/workflow-id) "|child")}
                input)]
    {:child/workflow-id (.workflowId handle)
     :child/result @handle}))
```

## Fan-out / scatter-gather

Start all children first, then deref — that's where the parallelism comes from. Route children through a queue so worker concurrency caps the fan-out:

```clojure
(defn fan-out [{:keys [queue]} dbos {:keys [n] :or {n 5}}]
  (let [items   (dbos/run-step dbos "gather-input" (vec (range 1 (inc n))))
        parent  (dbos/workflow-id)
        handles (mapv (fn [item]                       ; sequential mapv - required
                        (dbos/start-workflow!
                         dbos :myapp/process-item
                         {:workflow/id (str parent "|item-" item)
                          :workflow/queue queue}
                         {:item item}))
                      items)
        results (mapv deref handles)]                  ; await all
    (dbos/run-step dbos "aggregate"
      {:n (count results)
       :sum (reduce + (map :squared results))})))
```

Note: awaiting each child records an extra `DBOS.getResult` step — expected, shows up in `workflow-tree`.

## Scheduled (cron) workflows

Add `:workflow/schedule` to the definition. DBOS invokes the fn on each tick with `{:scheduled/at <Instant> :schedule/context <ctx>}` as `input`:

```clojure
(defn nightly-cleanup [deps dbos {:scheduled/keys [at]}]
  (dbos/run-step dbos "sweep-expired"
    (delete-expired! (:db deps) at))
  {:success true})

(def wf-definition
  {:workflow/key :myapp/nightly-cleanup
   :workflow/fn (partial nightly-cleanup {:db db})
   :workflow/schedule {:cron "0 0 3 * * *"}})   ; 03:00:00 daily
```

Critical details:

- **6-field cron** (`second minute hour day-of-month month day-of-week`), not 5-field crontab. `"*/2 * * * * *"` = every 2 seconds.
- Registration alone does NOT make it fire. Install the schedule row **after** `launch!`:

```clojure
(dbos/launch! instance)
(dbos/apply-schedules! instance definitions)   ; no-op for unscheduled defs
```

- Optional `:queue` in the schedule (`{:cron ".." :queue "my-queue"}`): with a queue, ticks are enqueued — run `apply-schedules!` on executor(s) listening on that queue. Without one, the schedule fires on the registering executor directly.

## Progress reporting with events

Events are a durable key/value channel on a running workflow. Classic use: report progress to a UI.

```clojure
;; workflow body - publish (durable, idempotent under replay, no step wrapper)
(defn ingest [dbos {:keys [items]}]
  (dbos/set-event! dbos :progress {:done 0 :total (count items)})
  ;; ... work ...
  (dbos/set-event! dbos :progress {:done (count items) :total (count items)})
  {:success true})

;; request handler / other process - read latest (nil if none yet)
(dbos/get-event instance-or-client "ingest-42" :progress)
;; => {:done 3 :total 10}
```

`set-event!` is body-only: it throws inside a step or outside a workflow.

## Web app + worker split

A common architecture: a lightweight worker process registers and runs workflows; the web app only enqueues.

**Worker** — registers the queue, listens on it:

```clojure
(def worker
  (dbos/create
   {:config    {:datasource db :app-name "my-app" :app-version app-version
                :listen-queues ["work-queue"]}
    :queues    [(-> (Queue. "work-queue") (.withWorkerConcurrency (int 8)))]
    :workflows [wf-definition]}))
(dbos/launch! worker)
```

**Web app** — a `DBOSClient` over the same database/schema/serializer:

```clojure
(def a-client (client/create-client {:datasource db :schema "dbos"}))
(client/enqueue-workflow! a-client :myapp/sync-user
                          {:workflow/id id :workflow/queue "work-queue"}
                          input)
```

Version pitfall: workflows only run on executors whose `app-version` matches the one recorded at dispatch. When enqueue side and worker are on different deploys, work can sit unclaimed — pin `{:workflow/app-version "x.y.z"}` or use `:latest`. Bump the executors' `:app-version` when workflow code changes so new executors don't pick up old-version workflows (see also DBOS patching for in-place upgrades).

## Integrant wiring

Pattern from the library's example app: each workflow definition is a component (deps injected via Integrant), collected with `#ig/refset`:

```clojure
;; workflows.clj - one ig/init-key per workflow, all deriving :dbos/workflow
(derive :myapp.workflow/sync-user :dbos/workflow)

(defmethod ig/init-key :myapp.workflow/sync-user [_ deps]
  {:workflow/key :myapp/sync-user
   :workflow/fn (partial sync-user deps)})

;; system.clj - the instance component registers everything and launches
(defmethod ig/init-key :dbos/instance
  [_ {:keys [datasource app-name workflows]}]
  (let [instance (dbos/create
                  {:config {:datasource datasource :app-name app-name}
                   :queues [(-> (Queue. "work-queue")
                                (.withWorkerConcurrency (int 4)))]
                   :workflows workflows})]
    (dbos/launch! instance)
    (dbos/apply-schedules! instance workflows)   ; after launch
    instance))

(defmethod ig/halt-key! :dbos/instance [_ instance]
  (when instance (dbos/shutdown! instance)))
```

```clojure
;; system.edn
{:example/datasource {:jdbc-url "..." :username "..." :password "..."}
 :myapp.workflow/sync-user {:db #ig/ref :example/datasource}
 :dbos/instance {:datasource #ig/ref :example/datasource
                 :app-name "my-app"
                 :workflows #ig/refset :dbos/workflow}}
```

The component returns the **raw DBOS instance** — the same object workflow bodies receive and `start-workflow!` takes; no wrapper map needed.
