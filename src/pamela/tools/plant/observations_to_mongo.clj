;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant.observations-to-mongo
  (:gen-class)
  (:require [pamela.tools.plant.interface :as pi]
            [pamela.tools.plant.connection :as pc]
            [pamela.tools.utils.rabbitmq :as rmq]
            [pamela.tools.utils.mongo.db :as mdb]
            [pamela.tools.utils.tpn-json :as tpn-json]

            [clojure.tools.cli :as cli]
            [clojure.string :as string]
            [clojure.pprint :refer :all]
            ))

; A application to log plant messages to mongo db
; Code to listen for plant observations and log to mongo db
; rmq host, port, exchange, plant-id/routing-key
; mongo host, port, db = plant-id, collection = observation-key of type [:heads :tails]

(def repl true)

(defonce plant-connection (atom nil))
(defonce plant-subscription (atom nil))

; TODO use dbname from command line if provided, otherwise plant-id
(defn handle-observation [data time]
  (when (= :observations (:state data))
    ;(pprint data)
    ;(pprint metadata)
    (println (:observations data))
    (let [db (mdb/get-db (name (:plant-id data)))
          docs (map (fn [doc]
                      (merge doc {:ts time})) (:observations data))]
      (mdb/insert-batch db "observations" docs))))

(defn handle-in-thread [data time]
  #_(future (do
              (println "Starting future to handle command" (Thread/currentThread))

              (handle-command (tpn.fromjson/map-from-json-str
                                (String. data "UTF-8"))
                              metadata)))
  (.start (Thread. (fn []
                     ;(println "Starting thread to handle command" (Thread/currentThread))
                     (handle-observation (tpn-json/map-from-json-str (String. data "UTF-8"))
                                         time)))))

(defn setup-subscription []
  (if-not @plant-subscription
    (let [subs (rmq/make-subscription "observations" (fn [_ _ data]
                                                       ;(println "Got data" data)
                                                       ;(println "Metadata" metadata)
                                                       ;(println "thread" (Thread/currentThread))
                                                       (handle-in-thread data (System/currentTimeMillis))
                                                       )
                                      (pi/get-channel @plant-connection)
                                      (pi/get-exchange @plant-connection))]
      ;(println "subscription" subs)
      (reset! plant-subscription subs)
      ;(println "Command Subscription")
      #_(pprint @plant-subscription))
    (println "Plant subscription exists")))

(defn cancel-subscription []
  (when @plant-subscription
    (rmq/cancel-subscription (:consumer-tag @plant-connection)
                             (pi/get-channel @plant-connection))
    (reset! plant-subscription nil)))

(defn reset-subscription []
  (when @plant-subscription
    (cancel-subscription)
    (setup-subscription)))

(defn setup [rmq-exch rmq-host rmq-port db-host db-port]
  (when-not (and rmq-exch rmq-host rmq-port db-host db-port)
    (println "setup need non nil values for rmq-exch rmq-host rmq-port db-name db-host db-port"
             rmq-exch rmq-host rmq-port db-host db-port))

  (when-not @plant-connection
    (reset! plant-connection (pc/make-plant-connection rmq-exch {:host rmq-host :port rmq-port}))
    ;(println "Plant Connection" plant-connection)
    )

  (if-not @plant-connection
    (println "Plant Connection is nil. Not setting up command subscription")
    (do
      (setup-subscription)
      (mdb/connect! :host db-host :port db-port))))

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default "pamela"]
                  ;["-i" "--plant-id plant-id" "Plant Id of this instance of the plant" :default "plant"]
                  [nil "--dbhost dbhost" "Mongo DB Host" :default "localhost"]
                  [nil "--dbport dbport" "Mongo DB Port" :default 27017 :parse-fn #(Integer/parseInt %)]
                  ;["-d" "--dbname dbname" "Mongo DB Name" :default "plant"]
                  ["-?" "--help"]])

(defn usage [options-summary]
  (->> ["Biased coin Plant"
        ""
        "Usage: java -jar plant-state-observer-0.1.2-SNAPSHOT-standalone.jar [options]"
        ""
        "Options:"
        options-summary
        ""]
       (string/join \newline)))

(defn exit []
  (when-not repl (System/exit 0)))

(defn -main [& args]
  (let [parsed (cli/parse-opts args cli-options)
        options (:options parsed)
        report-usage (or (:errors parsed) (:help (:options parsed)))
        ]
    (when report-usage
      (println (usage (:summary parsed)))
      (when (:errors parsed) (println (string/join \newline (:errors parsed))))
      (exit))

    (when-not report-usage
      (let [exchange (:exchange options)
            host (:host options)
            port (:port options)
            ;plant-id (:plant-id options)
            db-host (:dbhost options)
            db-port (:dbport options)
            ;db-name (:dbname options)
            ]
        (setup exchange host port db-host db-port)))))