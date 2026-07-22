(ns dbos.example.transit-time
  "A tiny set of java.time transit handlers, standing in for an app-wide
  handler set (a real app would reuse the same handlers it uses for HTTP /
  websocket payloads, e.g. luminus-transit).

  The library's serializer takes caller-injected handlers and boxes anything
  unhandled as a reconstructable `java-object`; giving it these handlers lets
  java.time values round-trip as first-class transit tags instead."
  (:require
   [cognitect.transit :as transit])
  (:import
   (java.time Instant LocalDate LocalDateTime)))

(def write-handlers
  "Class -> transit write-handler. Each java.time value is written under a
  custom tag as its ISO-8601 string."
  {Instant       (transit/write-handler (constantly "time/instant") str)
   LocalDate     (transit/write-handler (constantly "time/local-date") str)
   LocalDateTime (transit/write-handler (constantly "time/local-date-time") str)})

(def read-handlers
  "Tag -> transit read-handler, the inverse of `write-handlers`."
  {"time/instant"          (transit/read-handler #(Instant/parse %))
   "time/local-date"       (transit/read-handler #(LocalDate/parse %))
   "time/local-date-time"  (transit/read-handler #(LocalDateTime/parse %))})
