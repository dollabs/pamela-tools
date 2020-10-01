
;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.utils.rabbitmq
  (:require [pamela.tools.utils.tpn-json :as tpn-json]
            [pamela.tools.utils.util :as util]

            [langohr.core :as lcore]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.exchange :as le]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]

            [clojure.data.json :as cl-json]
            [clojure.pprint :refer :all])
  #_(:gen-class))

(def debug false)
(def default-exchange "tpn-updates"#_(str "test-" (System/getProperty "user.name")))
(def default-host "localhost")
(def default-port 5672)

(defn message-handler
  "RMQ message handler to pprint RMQ messages"
  [_ metadata ^bytes payload]
  (println "Received message metadata" metadata)
  (pprint (String. payload "UTF-8"))
  ;[ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  #_(println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                     (String. payload "UTF-8") metadata)))

(defn close-connection
  "Safe wrapper to close connection"
  [conn]
  (when (and conn (lcore/open? conn))
    (lcore/close conn)))

(defn close-channel
  "Safe wrapper to close channel"
  [ch]
  (when (and ch (lcore/open? ch))
    (lch/close ch)))

(defn make-channel
  "Creates a \"topic\" exchange with the given RMQ config
  Returns a map of :channel created-channel-object, :config given-or-{}, :exchange exch-name and :connection created-connection-object
  or nil if any error/exception"
  [exch-name rmq-config]
  (try
    (let [config (or rmq-config {})
          conn (lcore/connect config)
          ch (lch/open conn)
          exch-name (or exch-name default-exchange)]
      (le/declare ch exch-name "topic")
      {:channel ch :config config :exchange exch-name :connection conn})
    (catch java.net.ConnectException e (util/to-std-err (println (.getMessage e)
                                                                 (str "for " (:host rmq-config) ":" (:port rmq-config)))))))

(defn close-all
  "Safely closes channel and connection"
  [m]
  (close-connection (:connection m))
  (close-channel (:channel m)))

; According to javadocs, channels are not thread safe and could lead to bogus data being written to the channel
; but this issue hasn't presented itself in last few years.
(defn publish "Safe function to publish message"
  [data routing-key to-ch exch-name]
  {:pre [(string? data) (not (nil? routing-key)) (not (nil? to-ch)) (not (nil? exch-name))]}
  ;(println "publishing data" data)
  (let [r-key (or routing-key "#")]
    (if-not routing-key (util/to-std-err "routing-key is nil" exch-name data))
    (if (and to-ch exch-name)
      (lb/publish to-ch exch-name r-key data)
      (when debug (println "incomplete info for publishing data. channel-object and name" to-ch exch-name)))))

(defn make-subscription
  "Helper to create a subscription
  Returns {:routing-key sub-key-or-# :consumer-tag a-string :queue-name qname :message-handler given-handler-or-default-handler} "
  [sub-key msg-handler channel exch-name]
  (let [key     (or sub-key "#")
        handler (or msg-handler message-handler)
        aq      (if channel (lq/declare channel))
        qname   (if aq (.getQueue aq)
                       (println "Subscription Q is nil. Channel must be nil."))
        _       (if qname (lq/bind channel qname exch-name {:routing-key key})
                          (println "Q binding failed ch qname key" channel qname key))
        c-tag   (if qname (lc/subscribe channel qname handler {:auto-ack true}))]

    {:routing-key key :consumer-tag c-tag :queue-name qname :message-handler handler}))

(defn cancel-subscription "Safely cancel subscription"
  [^String consumer-tag channel]
  (when channel
    (langohr.basic/cancel channel consumer-tag)))

(defn publish-object
  "Publishes given clj data structure as json"
  [obj routing-key to-ch exch-name]
  {:pre [(map? obj)]}
  ;(println "publishing" routing-key exch-name)
  ;(clojure.pprint/pprint obj)
  (publish (cl-json/write-str obj) routing-key to-ch exch-name))

(defn publish-message [data app-id routing-key timestamp to-ch exch-name]
  (publish-object (merge {:app-id      app-id
                          :timestamp   timestamp
                          :routing-key routing-key}
                         data)
                  routing-key to-ch exch-name))

(defn payload-to-clj
  "Converts RMQ payload to clj data structure using our json to clj converter
  See 'pamela.tools.utils.tpn-json"
  [payload]
  (let [data (String. payload "UTF-8")
        js (tpn-json/map-from-json-str data)]
    js))

;;; Helper functions for various messages
;;; rmq-map is map object return by make-channel

(defn- activity-msg-helper [act-id network-id object-state routing-key rmq-map]
  (publish-object {:network-id network-id
                   act-id      {:uid              act-id
                                :tpn-object-state object-state}}
                  routing-key
                  (:channel rmq-map)
                  (:exchange rmq-map)))

(defn classic-activity-failed
  [act-id network-id rmq-map]
  (activity-msg-helper act-id network-id :failed "tpn.activity.finished" rmq-map))

(defn classic-activity-start [act-id network-id rmq-map]
  (activity-msg-helper act-id network-id :negotiation "tpn.activity.negotiation" rmq-map))