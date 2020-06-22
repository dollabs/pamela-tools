;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant.connection
  "RMQ connection interface abstraction and support for Plant messages"
  (:require [pamela.tools.plant.interface :refer :all]
            [pamela.tools.utils.rabbitmq :as mq]
            [pamela.tools.utils.timer :as timekeeper]
            [langohr.core :as lcore]
            [langohr.basic :as lb]

            [clojure.data.json :as cl-json]))

(def debug false)

(defn- publish [plant-connection routing-key msg]
  "plant-connection is defrecord, routing-key is string, msg is a clojure map"
  (lb/publish (:channel plant-connection) (:exchange plant-connection) routing-key (cl-json/write-str msg)))

(defn- finish-helper [plant-connection plant-id id reason timestamp state]
  "Helper function to create finish message"
  #_(println "finish helper" {:id        id
                              :plant-id  (or plant-id "plant")
                              :state     :finished
                              :timestamp (or timestamp (timekeeper/get-unix-time))
                              :reason    (assoc-in reason [:finish-state] state)}
             )
  (publish plant-connection "observations" {:id     id :plant-id (or plant-id "plant") :state :finished :timestamp (or timestamp (timekeeper/get-unix-time))
                                            :reason (assoc-in reason [:finish-state] state)}))

(defrecord plant-connection [exchange config]
  plant-rmq
  plantI                                                    ;implements plantI interface
  (setup-connection [plant-connection]
    (if-not exchange
      (println "plant-connection.setup-connection exchange is nil but value is required")
      (let [connection (if (and (:connection plant-connection) (lcore/open? (:connection plant-connection)))
                         (println "Connection exists:" plant-connection)
                         (mq/make-channel exchange (or config {})))]
        (merge plant-connection connection {:subscriptions []}))))

  (subscribe [this routing-key listener]
    (merge this {:subscriptions (conj (:subscriptions this)
                                      (mq/make-subscription routing-key listener (:channel this) (:exchange this)))}))

  (cancel-subscription [this]
    (let [a (first (:subscriptions this))
          other (into [] (rest (:subscriptions this)))]
      (when a
        (mq/cancel-subscription (:consumer-tag a) (:channel this)))
      (merge this {:subscriptions other})))

  (close-connection [plant-connection]
    (mq/close-connection (:connection plant-connection)))

  (get-exchange [_]
    exchange)

  (get-channel [pc]
    (:channel pc))

  (get-connection [pc]
    (:connection pc))

  (get-host [pc]
    (get-in pc [:config :host]))

  (get-port [pc]
    (get-in pc [:config :port]))

  (start [plant-connection plant-id id function-name args argsmap plant-part timestamp]

    (publish plant-connection (or plant-id "plant") {:id            id
                                                     :plant-id      (or plant-id "plant")
                                                     :state         :start
                                                     :function-name function-name
                                                     :args          args
                                                     :argsmap       argsmap :plant-part plant-part
                                                     :timestamp     (or timestamp (timekeeper/get-unix-time))}))

  (started [plant-connection plant-id id timestamp]
    (if debug
      (println "plant-id" plant-id "id" id "time-stamp" timestamp))
    (publish plant-connection "observations" {:id        id :plant-id (or plant-id "plant") :state :started
                                              :timestamp (or timestamp (timekeeper/get-unix-time))}))

  (status-update [plant-connection plant-id id completion-time percent-complete timestamp]
    (publish plant-connection "observations" (conj {:id              id :plant-id (or plant-id "plant")
                                                    :state           :status-update
                                                    :timestamp       (or timestamp (timekeeper/get-unix-time))
                                                    :completion-time completion-time}

                                                   (when percent-complete
                                                     {:percent-complete percent-complete}))))

  (finished [plant-connection plant-id id reason timestamp]
    (if debug
      (println "plant finished: plant-id" plant-id "id" id "reason  " reason "timestamp" timestamp))
    (finish-helper plant-connection plant-id id reason timestamp :success))

  (failed [plant-connection plant-id id reason timestamp]
    (finish-helper plant-connection plant-id id reason timestamp :failed))

  (cancelled [plant-connection plant-id id reason timestamp]
    (finish-helper plant-connection plant-id id reason timestamp :cancelled))

  (cancel [plant-connection plant-id id timestamp]
    (publish plant-connection (or plant-id "plant") {:id id :state :cancel :timestamp (or timestamp (timekeeper/get-unix-time))}))

  (observations [plant-connection plant-id id observations timestamp]
    (println "observations" observations)
    (if (pos? (count observations))
      (publish plant-connection "observations" {:id           id :plant-id (or plant-id "plant") :state :observations
                                                :observations observations
                                                :timestamp    (or timestamp (timekeeper/get-unix-time))}))))

(defn make-plant-connection [exchange config]
  (setup-connection (plant-connection. exchange config)))

(defn make-observation [field value & [timestamp]]
  (let [obs {:field field :value value}]
    (if timestamp
      (conj obs {:timestamp timestamp})
      obs)))

; Plant Interface messages and routing keys
; All keys and values required unless noted as optional
; Examples below are in clojure format.
; All timestamps are assumed to be unix time in millis since epoch (1 jan 1970)
; 1. Name of the method being invoked and args
; Published using id of the plant as routing key or #plant if there is no id.
;{:id 47 :state :start :pclass simpleplant :pmethod hardwork :args [10] :argsmap {:time 10}}

; 2. Message from the plant that the method has started
; published by all plants with routing key #observations
;{:id 47 :state :started :timestamp 12345}

; 3. Message from the plant providing status update for the method
; :percent-complete is optional
; published by all plants with routing key #observations
;{:id 47 :state :status-update :timestamp 12345 :estimated-completion-time 12345 :percent-complete 12}

; 4. Message from the plant that the method has finished successfully
; :reason and :finish-state are required.
; Other key value pairs for :reason are optional
; published by all plants with routing key #observations
;{:id 47 :state :finished :timestamp 12345 :reason {:finish-state :success
;                                                   :str          "Finished Successfully"
;                                                   }}

; 5. Message from the plant that the method has finished with a failure
; :reason and :finish-state are required.
; Other key value pairs for :reason are optional
; published by all plants with routing key #observations
;{:id 47 :state :finished :timestamp 12345 :reason {:finish-state :failed
;                                                   :str          "file not found"}}

; 6. Message to the plant to cancel method execution
; Published using id of the plant as routing key or #plant if there is no id.
;{:id 47 :state :cancel}

; 7. Message from the plant that the method has been cancelled
; :reason and :finish-state are required.
; Other key value pairs for :reason are optional
; published by all plants with routing key #observations
;{:id 47 :state :finished :timestamp 12345 :reason {:finish-state :cancelled
;                                                   :str          "Cancelled per user request"}}

; 8. Plant observations
; An observation message must have atleast one observation
; published by all plants with routing key #observations
;{:id 47
; :state :observations
; ## Each Element of observations array is some observation
; :observations [{:field :ball-in-motion :value true :timestamp 12345}
;                {:field :ready :value false :timestamp 12345}
;               ]}