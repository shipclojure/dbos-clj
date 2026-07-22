(ns dbos.serializer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [cognitect.transit :as transit]
   [dbos.serializer :as serializer])
  (:import
   (dev.dbos.transact.json DBOSSerializer)
   (java.io File)
   (java.net URI)
   (java.time Duration Instant LocalDate LocalDateTime LocalTime
              OffsetDateTime YearMonth ZonedDateTime)
   (java.time.format DateTimeFormatter)))

;; Test-local java.time handlers (a copy of the luminus-transit set). The
;; library bundles none; callers inject their own. These stand in for an
;; app's `*.common.transit.time` handlers to prove first-class java.time
;; fidelity when handlers are supplied.

(def ^:private iso-local-time (DateTimeFormatter/ofPattern "HH:mm:ss.SSS"))
(def ^:private iso-local-date (DateTimeFormatter/ofPattern "yyyy-MM-dd"))
(def ^:private iso-local-date-time (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSS"))
(def ^:private iso-zoned-date-time (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSSXX"))
(def ^:private iso-year-month (DateTimeFormatter/ofPattern "yyyy-MM"))

(def ^:private write-handlers
  {Instant       (transit/write-handler (constantly "t") #(.toString ^Instant %))
   LocalTime     (transit/write-handler (constantly "LocalTime") #(.format ^LocalTime % iso-local-time))
   LocalDate     (transit/write-handler (constantly "LocalDate") #(.format ^LocalDate % iso-local-date))
   LocalDateTime (transit/write-handler (constantly "LocalDateTime") #(.format ^LocalDateTime % iso-local-date-time))
   ZonedDateTime (transit/write-handler (constantly "ZonedDateTime") #(.format ^ZonedDateTime % iso-zoned-date-time))
   YearMonth     (transit/write-handler (constantly "YearMonth") #(.format ^YearMonth % iso-year-month))})

(def ^:private read-handlers
  {"t"             (transit/read-handler #(Instant/parse %))
   "LocalTime"     (transit/read-handler #(LocalTime/parse % iso-local-time))
   "LocalDate"     (transit/read-handler #(LocalDate/parse % iso-local-date))
   "LocalDateTime" (transit/read-handler #(LocalDateTime/parse % iso-local-date-time))
   "ZonedDateTime" (transit/read-handler #(ZonedDateTime/parse % iso-zoned-date-time))
   "YearMonth"     (transit/read-handler #(YearMonth/parse % iso-year-month))})

(defn- round-trip
  "Round-trip with the test-local time handlers injected."
  [v]
  (serializer/deserialize (serializer/serialize v write-handlers) read-handlers))

(defrecord SampleRecord [id name])

(deftest serialize-deserialize-test
  (testing "round-trips rich clojure data unchanged (with injected time handlers)"
    (let [sample {:org/id (random-uuid)
                  :date (LocalDate/parse "2026-07-16")
                  :at (Instant/parse "2026-07-16T10:00:00Z")
                  :local-dt (LocalDateTime/parse "2026-07-16T10:00:00.123")
                  :month (YearMonth/parse "2026-07")
                  :amounts [1.5 2M 3/4]
                  :tags #{:openai :anthropic}
                  :nested {:deep [{:a nil}]}}]
      (is (= sample (round-trip sample)))))

  (testing "produces persistent collections, not mutated POJO-style structures"
    (let [result (round-trip {:tags #{:a} :items [{:k :v}]})]
      (is (instance? clojure.lang.IPersistentMap result))
      (is (set? (:tags result)))
      (is (vector? (:items result)))
      (is (keyword? (-> result :items first :k)))))

  (testing "handles nil and scalars"
    (is (nil? (serializer/deserialize nil)))
    (is (nil? (round-trip nil)))
    (is (= 42 (round-trip 42)))
    (is (= "hello" (round-trip "hello"))))

  (testing "deserialize of a non-transit string throws - corrupt rows surface"
    (is (thrown? RuntimeException (serializer/deserialize "not transit at all")))))

(deftest no-handler-round-trip-test
  (testing "called with no handlers, plain Clojure data still round-trips"
    (let [sample {:org/id (random-uuid)
                  :amounts [1.5 2M 3/4]
                  :tags #{:openai :anthropic}
                  :nested {:deep [{:a nil}]}
                  :kw :some/keyword
                  :s "text"
                  :n 42}]
      (is (= sample (serializer/deserialize (serializer/serialize sample))))))

  (testing "with no handlers, java.time falls through to the java-object box"
    ;; Instant has no compact tag without handlers, so it is boxed and
    ;; reconstructed via Jackson to an instant-equal value.
    (let [original (Instant/parse "2026-07-16T10:00:00Z")
          back (serializer/deserialize (serializer/serialize original))]
      (is (instance? Instant back))
      (is (= original back)))))

(deftest java-object-boxing-test
  (testing "unknown types Jackson can round-trip reconstruct to equal instances"
    (let [file (round-trip (File. "/tmp/x"))]
      (is (instance? File file))
      (is (= (File. "/tmp/x") file)))
    (let [duration (round-trip (Duration/ofSeconds 5))]
      (is (instance? Duration duration))
      (is (= (Duration/ofSeconds 5) duration))))

  (testing "java.net.URI has a built-in transit handler, never boxed"
    ;; Transit handles URIs natively (tag "r"); they read back as
    ;; com.cognitect.transit.impl.URIImpl - pre-existing, not boxing.
    (is (= "https://example.com/x"
           (str (round-trip (URI. "https://example.com/x"))))))

  (testing "nested unknown type reconstructs in place, siblings intact"
    (let [result (round-trip {:ok 1 :items [1 2] :bad (File. "/tmp/x")})]
      (is (= 1 (:ok result)))
      (is (= [1 2] (:items result)))
      (is (= (File. "/tmp/x") (:bad result)))))

  (testing "non-reconstructable values make serialize throw - fail fast"
    ;; (atom 1) serializes through Jackson but fails to deserialize, so the
    ;; write-side round-trip probe rejects it and serialize throws.
    (is (thrown? clojure.lang.ExceptionInfo (serializer/serialize (atom 1))))
    (try
      (serializer/serialize (atom 1))
      (is false "expected serialize to throw")
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (is (= :dbos.serializer/unserializable (:error/type data)))
          (is (= "clojure.lang.Atom" (:class data)))))))

  (testing "nested non-reconstructable value fails the whole serialize call"
    (is (thrown? clojure.lang.ExceptionInfo
                 (serializer/serialize {:ok 1 :bad (atom 1)}))))

  (testing "fns reconstruct to working instances"
    ;; Jackson bean-maps fn classes and they have no-arg constructors, so
    ;; `inc` round-trips to a callable fn instance.
    (is (= 2 ((round-trip inc) 1))))

  (testing "OffsetDateTime reconstructs instant-equal, offset may normalize"
    ;; No OffsetDateTime handler in the injected set, so it is boxed. Jackson
    ;; reconstructs the same instant but normalizes the offset to UTC, so `=`
    ;; to the original may be false while .isEqual is true.
    (let [original (OffsetDateTime/parse "2026-07-16T10:00:00+02:00")
          back (round-trip original)]
      (is (instance? OffsetDateTime back))
      (is (.isEqual original ^OffsetDateTime back))))

  (testing "reconstruct failure at read time throws - fail fast"
    ;; A box whose class is missing/evolved on this JVM must not flow into
    ;; workflow steps as a map where an object should be.
    (let [wire (str "{\"~#java-object\":"
                    "{\"~:java-object/class\":\"com.example.Gone\","
                    "\"~:java-object/repr\":\"gone\","
                    "\"~:java-object/jackson\":"
                    "\"[\\\"com.example.Gone\\\",\\\"x\\\"]\"}}")]
      (is (thrown? clojure.lang.ExceptionInfo (serializer/deserialize wire)))
      (try
        (serializer/deserialize wire)
        (is false "expected deserialize to throw")
        (catch clojure.lang.ExceptionInfo e
          (let [data (ex-data e)]
            (is (= :dbos.serializer/reconstruct-failed (:error/type data)))
            (is (= "com.example.Gone"
                   (-> data :java-object :java-object/class))))))))

  (testing "defrecords degrade to plain maps (pre-existing behavior)"
    ;; Records satisfy transit's map handler, so the type is lost but the
    ;; keys/values survive. Documented, not boxed.
    (let [result (round-trip (->SampleRecord 1 "a"))]
      (is (= {:id 1 :name "a"} result))
      (is (not (instance? SampleRecord result)))))

  (testing "unknown foreign tag on read yields tag/rep data, not TaggedValueImpl"
    (is (= {:transit/tag "mystery" :transit/rep 42}
           (serializer/deserialize "{\"~#mystery\":42}"))))

  (testing "bare Object serializes as null, bypassing the box"
    ;; transit resolves a handler for java.lang.Object itself before the
    ;; default handler is ever consulted, so a bare Object serializes as
    ;; null - {"~#'":null} at top level (quote-wrapped), plain null nested -
    ;; and round-trips to nil. Pre-existing quirk, unchanged by boxing.
    (is (= "{\"~#'\":null}" (serializer/serialize (Object.))))
    (is (nil? (round-trip (Object.))))
    (is (= {:o nil} (round-trip {:o (Object.)}))))

  (testing "injected handlers don't affect handled types"
    (let [sample {:id (random-uuid)
                  :at (Instant/parse "2026-07-16T10:00:00Z")
                  :kw :some/keyword}]
      (is (= sample (round-trip sample))))))

(deftest dbos-serializer-interface-test
  (let [^DBOSSerializer ser (serializer/transit-serializer
                             {:write-handlers write-handlers
                              :read-handlers read-handlers})]
    (testing "name is the frozen format identifier"
      (is (= "transit_json_verbose" (.name ser))))

    (testing "value round-trip through the interface"
      (let [sample {:a [1 2 3] :b :kw :at (Instant/parse "2026-07-16T10:00:00Z")}]
        (is (= sample (.deserialize ser (.serialize ser sample))))))

    (testing "throwable round-trip preserves message and ex-data"
      (let [ex (ex-info "boom" {:cause :test :n 42})
            result (.deserializeThrowable ser (.serializeThrowable ser ex))]
        (is (instance? Throwable result))
        (is (= "boom" (ex-message result)))
        (is (= {:cause :test
                :n 42
                :ex/original-class "clojure.lang.ExceptionInfo"}
               (ex-data result)))))

    (testing "plain java exceptions serialize too"
      (let [result (->> (IllegalStateException. "nope")
                        (.serializeThrowable ser)
                        (.deserializeThrowable ser))]
        (is (= "nope" (ex-message result)))
        (is (= "java.lang.IllegalStateException"
               (:ex/original-class (ex-data result))))))

    (testing "nil throwable text deserializes to nil"
      (is (nil? (.deserializeThrowable ser nil)))))

  (testing "no-arg transit-serializer still constructs and round-trips plain data"
    (let [^DBOSSerializer ser (serializer/transit-serializer)]
      (is (= "transit_json_verbose" (.name ser)))
      (is (= {:a 1} (.deserialize ser (.serialize ser {:a 1})))))))
