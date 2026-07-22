(ns dbos.example.serializer
  "The example's DBOS serializer: the transit json-verbose serializer from
  the library, pre-wired with this app's java.time handlers so DBOS rows
  round-trip java.time values as first-class transit tags. The same
  serializer must be given to both the DBOS instance and any DBOSClient so
  the two agree on the wire format."
  (:require
   [dbos.example.transit-time :as tt]
   [dbos.serializer :as ser]))

(defn transit-serializer
  "A `DBOSSerializer` backed by transit json-verbose with this app's
  java.time handlers, for `DBOSConfig.withSerializer` / the client."
  []
  (ser/transit-serializer {:write-handlers tt/write-handlers
                           :read-handlers tt/read-handlers}))
