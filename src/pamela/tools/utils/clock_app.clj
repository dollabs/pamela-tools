;
; The software contained herein is proprietary and
; a confidential trade secret of Dynamic Object Language Labs Inc.
; Copyright (c) 2020.  All rights reserved.
;

; Application to simulate plant methods

(ns pamela.tools.utils.clock-app
  ^{:doc "An application that simulates clock"}
  (:gen-class)
  (:require [pamela.tools.utils.rabbitmq :as rmq]
            [ruiyun.tools.timer :as timer]

            [clojure.pprint :refer :all]
            [clojure.tools.cli :as cli]
            [clojure.string :as string]))

(def repl true)
(defonce connection-info nil)
(defonce clock-timer (timer/deamon-timer "Clock generator"))

(defn reset-timer []
  (if clock-timer (timer/cancel! clock-timer))
  (def clock-timer (timer/deamon-timer "Clock generator")))

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default "tpn-updates"]
                  ["-c" "--simulate-clock Frequency" "Will publish clock messages to rkey `clock` at given frequency" :default 2]
                  ["-?" "--help"]])

#_(defn publish [id state net-id routing-key]
  (println "publish " id state net-id routing-key)
  (rmq/publish-object {:network-id net-id
                       id          {:uid              id
                                    :tpn-object-state state}}
                      routing-key ))

(defn usage [options-summary]
  (->> ["Clock generator app"
        ""
        "Usage: java -jar clock-generator.jar [options]"
        ""
        "Options:"
        options-summary]
       (string/join \newline)))

(defn print-help [summary-options]
  (println (usage summary-options)))

(defn exit [code]
  (when-not repl
    (System/exit code))
  (if repl
    (println "in repl mode. Not exiting")))

(defn reset-rmq []
  (when connection-info
    (println "Reset RMQ")
    (rmq/close-connection (:connection connection-info))))

(defn publish-clock []
  ;(println "Publishing clock")
  (rmq/publish-object {:app-id "Clock Generator"
                       :timestamp (System/currentTimeMillis)
                       } "clock" (:channel connection-info) (:exchange connection-info))
  )

(defn schedule-timer [period]
  (println "Scheduling clock publish every " period "millis")
  (timer/run-task! (fn []
                     (publish-clock)) :by clock-timer :period period))

(defn -main [& args]
  (let [parsed (cli/parse-opts args cli-options)
        opts (:options parsed)
        ;_ (pprint opts)
        help (:help opts)
        conn-info (rmq/make-channel (:exchange opts) opts)
        sim-clock (:simulate-clock opts)
        ]

    (def repl false)
    (when (:errors parsed)
      (print-help (:summary parsed))
      (println (string/join \newline (:errors parsed)))
      (exit 0))

    (when help
      (print-help (:summary parsed))
      (exit 0))

    (if-not conn-info (exit 0))

    (when (> sim-clock 0)
      (schedule-timer (long (* 1000 (float (/ 1 sim-clock))))))
    ; Cleanup previous connection
    (reset-rmq)

    (def connection-info conn-info)
    (println "App State")
    (println "----------")
    (clojure.pprint/pprint connection-info)
    (println "----------")))