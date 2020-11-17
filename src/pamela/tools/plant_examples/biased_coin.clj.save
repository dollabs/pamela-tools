;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant-examples.biased-coin
  "Implementation of biased coin example"
  (:gen-class)
  (:require [pamela.tools.plant.interface :as pi]
            [pamela.tools.plant.connection :as pc]
            [pamela.tools.plant-examples.hmm :as hmm]
            [pamela.tools.utils.rabbitmq :as rmq]
            [pamela.tools.utils.tpn-json :as tpn-json]

            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.pprint :refer :all]
            [pamela.tools.utils.mongo.db :as mdb]
            )
  (:import (java.util Random)))

(def coin-model {
                 ;probability of initial state
                 :pi           {:biased 0.5 :unbiased 0.5}

                 ;state transition probabilities
                 :transitions  {
                                :biased   {:biased 0.99 :unbiased 0.01}
                                :unbiased {:unbiased 0.99 :biased 0.01}
                                }

                 ;emission probabilities
                 :emissions    {
                                :biased   {:heads 0.9 :tails 0.1}
                                :unbiased {:heads 0.50 :tails 0.50}}

                 :var-bindings {:biased   {:transtions {:unbiased "tBU" :biased "tBB"}
                                           :emissions  {:tail "eBT" :head "eBH"}
                                           }
                                :unbiased {:transitions {:unbiased "tUU" :biased "tUB"}
                                           :emissions   {:tail "eUT" :head "eUH"}
                                           }}})

(defonce coin (hmm/make-model coin-model))
(defonce coin-state (atom (:initial-state coin)))
(defonce choose-coin-rng (Random. 7 #_(System/nanoTime)))
(defonce flip-rng (Random. 11 #_(System/nanoTime)))

(def repl true)
(def debug false)

(defonce plant-connection (atom nil))
(defonce command-subscription (atom nil))

(defn choose-coin! []
  (let [current-state @coin-state]
    (reset! coin-state (hmm/choose-with-probability (current-state (:tr-p-range coin))
                                                    choose-coin-rng))
    (if debug
      (println "choosing coin current state" current-state "->" @coin-state "changed" (not= current-state @coin-state)))

    (if (not= current-state @coin-state)
      (println "coin state changed:" current-state "->" @coin-state))
    )
  @coin-state)

(defn flip []
  (hmm/choose-with-probability ((choose-coin!) (:em-p-range coin)) flip-rng))

(defn handle-command [data metadata]
  ;(pprint data)
  ;(pprint metadata)

  (when (= :start (:state data))
    (let [face      (flip)
          new-state @coin-state
          ; observation model is {:field :field-name :value :field-value :other :k}
          obs       [{:field :observed-face :value face} {:field :chosen-coin :value new-state}] #_(if (= prev-state new-state)
                                                                     {:observed-face face}
                                                                     {:observed-face face :chosen-coin new-state})]

      (pi/started @plant-connection (:routing-key metadata) (:id data) nil)
      (pi/observations @plant-connection (:routing-key metadata) (:id data) obs nil)
      (pi/finished @plant-connection (:routing-key metadata) (:id data) {:reason "flip success"} nil))))

(defn create-command-thread [data metadata]
  #_(future (do
              (println "Starting future to handle command" (Thread/currentThread))

              (handle-command (tpn.fromjson/map-from-json-str
                                (String. data "UTF-8"))
                              metadata)))
  (.start (Thread. (fn []
                     ;(println "Starting thread to handle command" (Thread/currentThread))
                     (handle-command (tpn-json/map-from-json-str (String. data "UTF-8"))
                                     metadata)))))

(defn setup-command-subscription [plant-id]
  (if-not @command-subscription
    (let [subs (rmq/make-subscription plant-id (fn [_ metadata data]
                                                 ;(println "Got data" data)
                                                 ;(println "Metadata" metadata)
                                                 ;(println "thread" (Thread/currentThread))
                                                 (create-command-thread data metadata)
                                                 )
                                      (pi/get-channel @plant-connection)
                                      (pi/get-exchange @plant-connection))]
      ;(println "subscription" subs)
      (reset! command-subscription subs)
      ;(println "Command Subscription")
      #_(pprint @command-subscription))
    (println "Command subscription exists")))

(defn cancel-subscription []
  (when @command-subscription
    (rmq/cancel-subscription (:consumer-tag @command-subscription)
                             (pi/get-channel @plant-connection))
    (reset! command-subscription nil)))

(defn reset-subscription []
  (when @command-subscription
    (let [routing-key (:routing-key @command-subscription)]
      (cancel-subscription)
      (setup-command-subscription routing-key)
      )))

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default "pamela"]
                  ["-i" "--plant-id plant-id" "Plant Id of this instance of the plant" :default "coin-plant"]
                  ["-m" "--model json file containing the model" "If specified, model to be used by the plant. Otherwise default will be used"]
                  ["-?" "--help"]])

(defn usage [options-summary]
  (->> ["Biased coin Plant"
        ""
        "Usage: java -jar coin-plant-0.1.2-SNAPSHOT-standalone.jar [options]"
        ""
        "Options:"
        options-summary
        ""]
       (str/join \newline)))

(defn exit []
  (when-not repl (System/exit 0)))

(defn setup [model plant-id rmq-exchange rmq-host rmq-port]
  #_(println "Setting up coin plant with plant-id" plant-id "rmq-exchange" rmq-exchange
             "rmq-host" rmq-host "rmq-port" rmq-port "model" model)
  ;(println "Plant Connection object" plant-connection)
  ;(println "Command subscription object" command-subscription)

  (when-not (= model coin-model)
    (def coin (hmm/make-model model)))
  #_(println "Using Coin model" coin)
  (println "transitions and emissions")
  (pprint {:transitions (:transitions coin)
           :emissions   (:emissions coin)
           :tr-index    (:tr-index coin)
           :em-index    (:em-index coin)})
  (if-not (and plant-id rmq-exchange rmq-host rmq-port)
    (println "Need non nil values for rmq setup plant-id" plant-id "rmq-exchange" rmq-exchange
             "rmq-host" rmq-host "rmq-port" rmq-port))

  (when-not @plant-connection
    (reset! plant-connection (pc/make-plant-connection rmq-exchange {:host rmq-host :port rmq-port}))
    ;(println "Plant Connection" plant-connection)
    )

  (if-not @plant-connection
    (println "Plant Connection is nil. Not setting up command subscription")
    (setup-command-subscription plant-id)))

(defn read-model-from-file [filename]
  (if filename
    (tpn-json/from-file filename)))

(defn -main [& args]
  (let [parsed       (cli/parse-opts args cli-options)
        options      (:options parsed)
        report-usage (or (:errors parsed) (:help (:options parsed)))]

    (when report-usage
      (println (usage (:summary parsed)))
      (when (:errors parsed) (println (str/join \newline (:errors parsed))))
      (exit))

    (when-not report-usage
      (let [exchange (:exchange options)
            host     (:host options)
            port     (:port options)
            plant-id (:plant-id options)
            model    (read-model-from-file (:model options))
            model    (if-not model coin-model model)
            ]

        (setup model plant-id exchange host port)))))

(defn collect-samples [count]
  (repeatedly count flip))

(defn count-state-changes [states]
  (merge {:frequencies (frequencies states)}
         (reduce (fn [r state]
                   (if (= state (:last r))
                     r
                     {:last state :count (inc (:count r))})
                   ) {:count 0} states)))

(defn test-coin-state-change [count]
  (count-state-changes (map (fn [_]
                              (choose-coin!)) (range count))))

(defn test-flip-state-change [count]
  (count-state-changes (map (fn [_]
                              (flip)) (range count))))

(defn insert-model-in-db [host port plant-id]
  (mdb/connect! :host host :port port)
  (mdb/insert plant-id "plant-ground-truth" coin)
  (mdb/shutdown))

(defn reset-plant-connection []
  (let [plant-id     (:routing-key @command-subscription)
        rmq-exchange (:exchange @plant-connection)
        host         (pi/get-host @plant-connection)
        port         (pi/get-port @plant-connection)]
    (when @plant-connection
      (cancel-subscription)
      (pi/close-connection @plant-connection)
      (reset! plant-connection nil)
      (setup coin-model plant-id rmq-exchange host port))))