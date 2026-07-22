(ns dbos.serializer
  (:require
   [cognitect.transit :as transit]
   [taoensso.trove :as trove])
  (:import
   (com.cognitect.transit DefaultReadHandler)
   (dev.dbos.transact.json DBOSJavaSerializer DBOSSerializer)
   (java.io ByteArrayInputStream ByteArrayOutputStream)))

(def serializer-name
  "Format id recorded in every DBOS row. Frozen — never change once workflow
  data exists."
  "transit_json_verbose")

(defn- jackson-payload
  [value]
  (try
    (let [json (.serialize DBOSJavaSerializer/INSTANCE value)]
      (.deserialize DBOSJavaSerializer/INSTANCE json)
      json)
    (catch Exception cause
      (throw (ex-info (str "Value cannot be durably serialized: no transit "
                           "handler and Jackson cannot round-trip it - fix "
                           "the workflow/step to return serializable data")
                      {:error/type :dbos.serializer/unserializable
                       :class (.getName (class value))
                       :repr (pr-str value)}
                      cause)))))

(def ^:private write-default-handler
  (transit/write-handler
   (fn [_] "java-object")
   (fn [value]
     (let [jackson-json (jackson-payload value)]
       (trove/log! {:level :warn
                    :id :dbos.serializer/java-object-boxed
                    :data {:class (.getName (class value))}
                    :msg "Boxing value with no transit handler as java-object"})
       {:java-object/class (.getName (class value))
        :java-object/repr (pr-str value)
        :java-object/jackson jackson-json}))))

(defn- reconstruct-java-object
  "Rebuild the original object from a java-object box's :java-object/jackson
  payload. Throws when the class is missing/evolved on this JVM (fail loud
  rather than hand a step a map where an object should be)."
  [{:java-object/keys [jackson] :as box}]
  (try
    (.deserialize DBOSJavaSerializer/INSTANCE jackson)
    (catch Exception cause
      (throw (ex-info "Cannot reconstruct java-object box - class missing or evolved on this JVM"
                      {:error/type :dbos.serializer/reconstruct-failed
                       :java-object box}
                      cause)))))

(def ^:private read-default-handler
  "Reconstructs java-object boxes; any other unknown tag surfaces as
  `{:transit/tag .. :transit/rep ..}` data."
  (reify DefaultReadHandler
    (fromRep [_ tag rep]
      (if (= tag "java-object")
        (reconstruct-java-object rep)
        {:transit/tag tag :transit/rep rep}))))

(defn- unwrap-and-rethrow!
  "Transit wraps write/read-handler exceptions in a bare RuntimeException;
  rethrow our marked ex-infos so callers get the original ex-data."
  [^RuntimeException e]
  (if (some-> (ex-cause e) ex-data :error/type namespace (= "dbos.serializer"))
    (throw (ex-cause e))
    (throw e)))

(defn serialize
  "Serialize a Clojure value to a Transit json-verbose string. `write-handlers`
  is optional (caller-injected, e.g. java.time); unhandled types fall through
  to the java-object box. Throws :dbos.serializer/unserializable when a value
  has no handler and Jackson can't round-trip it (fail fast)."
  ([input] (serialize input nil))
  ([input write-handlers]
   (let [baos (ByteArrayOutputStream.)
         writer (transit/writer baos :json-verbose
                                {:handlers write-handlers
                                 :default-handler write-default-handler})]
     (try
       (transit/write writer input)
       (catch RuntimeException e
         (unwrap-and-rethrow! e)))
     (.toString baos "UTF-8"))))

(defn deserialize
  "Deserialize a Transit json-verbose string back to a Clojure value; nil for
  nil input. `read-handlers` is optional (caller-injected). Throws on
  unreadable input and on failed java-object reconstruction
  (:dbos.serializer/reconstruct-failed) — a corrupt row must fail loudly."
  ([transit-str] (deserialize transit-str nil))
  ([transit-str read-handlers]
   (when transit-str
     (try
       (-> (ByteArrayInputStream. (.getBytes ^String transit-str "UTF-8"))
           (transit/reader :json-verbose
                           {:handlers read-handlers
                            :default-handler read-default-handler})
           (transit/read))
       (catch RuntimeException e
         (unwrap-and-rethrow! e))))))

(defn- throwable->data
  [^Throwable throwable]
  {:ex/class (.getName (class throwable))
   :ex/message (ex-message throwable)
   :ex/data (ex-data throwable)})

(defn- data->throwable
  [{:ex/keys [class message data]}]
  (ex-info (or message "DBOS workflow error")
           (assoc (or data {}) :ex/original-class class)))

(defn transit-serializer
  "DBOSSerializer backed by Transit json-verbose, for `DBOSConfig.withSerializer`.
  Caller supplies :write-handlers / :read-handlers (e.g. app-wide java.time
  handlers); anything unhandled falls through to the java-object box."
  (^DBOSSerializer [] (transit-serializer nil))
  (^DBOSSerializer [{:keys [write-handlers read-handlers]}]
   (reify DBOSSerializer
     (name [_] serializer-name)
     (serialize [_ v] (serialize v write-handlers))
     (deserialize [_ t] (deserialize t read-handlers))
     (serializeThrowable [_ th] (serialize (throwable->data th) write-handlers))
     (deserializeThrowable [_ t] (some-> t (deserialize read-handlers) data->throwable)))))
