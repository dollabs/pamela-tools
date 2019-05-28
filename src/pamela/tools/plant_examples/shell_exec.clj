;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant-examples.shell-exec
  "Main file that implements plant interface for exec-plant.pamela"
  (:gen-class)
  (:require [pamela.tools.plant.interface :as pi]
            [pamela.tools.plant-examples.command :as pl-cmd]

            [pamela.tools.plant.connection :as pl-con]
            [pamela.tools.plant-examples.exec :as pl-exec]
            [pamela.tools.plant-examples.scp :as pl-scp]
            [pamela.tools.utils.tpn-json :as tpn-json]
            [pamela.tools.utils.rabbitmq :as rmq]
            [ruiyun.tools.timer :as timer]

            [clojure.tools.cli :as cli]
            [clojure.string :as string]
            [clojure.pprint :refer :all]
            [clojure.core.async :as async]))

(def debug true)
(def repl true)

(def plant-functions #{"exec" "scp-upload"})

(defonce state (atom {}))
;(defonce commands-channel (async/chan))

(defn update-state!
  ([m]
   (swap! state merge m))
  ([k v]
   (update-state! {k v})))

(defn get-plant-id []
  (get-in @state [:plant-commands-subscription :routing-key]))

(defn is-command-running? [id]
  (let [current-state (get-in @state [id :state])]
    (if current-state
      (not= :finished current-state)
      false)))

(defn send-status [id]

  (println "Send status for" id)
  (let [obj (id @state)
        is-running (is-command-running? id)
        pl-connection (:plant-interface @state)
        status (if is-running (pl-cmd/get-status obj) {})
        completion-time (:completion-time status)
        ]
    (when is-running
      (if-not completion-time
        (println "Status for id" id "is" status ". :completion-time is required")
        (pi/status-update pl-connection (get-plant-id) id
                          completion-time (:percent-complete status) nil)))))

(defn start-sending-status-update [obj]
  (let [id (pl-cmd/id obj)
        timer-obj (timer/timer (str "Status timer for: " id))]
    (println "Setup status timer for" id)
    (swap! state assoc-in [:status-timers id] timer-obj)
    (timer/run-task! #(send-status id) :delay 2000 :period 2000 :by timer-obj)))

(defn stop-sending-status-update [obj]
  (let [id (pl-cmd/id obj)
        mytimer (get-in @state [:status-timers id])]
    (when mytimer
      (println "Cancelling status timer for" id)
      (timer/cancel! mytimer))))

(defn do-command-finished [id exitValue other-m]
  (println "command-finished" id (pl-cmd/command (id @state)))
  ;(pprint (id @state))

  (let [cmd (id @state)
        current-state (pl-cmd/get-state cmd)
        status-map (merge other-m {:exit-value exitValue})

        ;cmd (pl-cmd/update-state! cmd :finished)
        cmd (merge cmd status-map)
        finished-block (fn []
                         (if (= 0 exitValue)
                           (pi/finished (:plant-interface @state) (get-plant-id) id status-map nil)
                           (pi/failed (:plant-interface @state) (get-plant-id) id status-map nil))
                         (update-state! id (pl-cmd/update-state! cmd :finished)))]
    (stop-sending-status-update cmd)
    (cond (= :started current-state)
          (finished-block)

          (= :cancel current-state)
          (do
            (pi/cancelled (:plant-interface @state) (get-plant-id) id status-map nil)
            (update-state! id (pl-cmd/update-state! cmd :cancelled)))

          :else
          (do (println "Unknown current state of command:" id current-state)
              (finished-block)))))

(defn do-command-start [command-object]
  (println "Starting command:" (pl-cmd/command command-object))
  (update-state! (pl-cmd/id command-object) (pl-cmd/start-command command-object))
  (pi/started (:plant-interface @state) (get-plant-id) (pl-cmd/id command-object) nil)
  (if (:can-send-status command-object)
    (start-sending-status-update command-object)))

(defn make-command-object [command-map]
  (let [func (:function-name command-map)]
    (cond (= "exec" func)
          (pl-exec/make-exec-command do-command-finished command-map)

          (= "scp-upload" func)
          (merge (pl-scp/make-scp-upload do-command-finished command-map)
                 {:can-send-status true})

          :else
          (println "Cannot make unknown command func" func))))

(defn handle-command-start [command-msg plant-id]
  (let [id (:id command-msg)
        func (:function-name command-msg)
        args (:args command-msg)]
    (cond (not (contains? plant-functions func))
          (do
            (println "plant-function not supported" func args)
            (pi/failed (:plant-interface @state) plant-id id {:command-status (str "Function not supported " func)} nil)
            (update-state! {id {:state :finished :command-status (str "Function not supported " func)}}))

          (is-command-running? id)
          (do
            (println "command" id "is already running. Ignoring"))

          :else
          (do-command-start (make-command-object command-msg)))))

; TODO test cancel command via RMQ
(defn handle-command-cancel [command-msg]
  (let [cmd ((:id command-msg) @state)]
    ;; Update :cancel state here so that finish callback knows the reason for command being finished
    (update-state! (pl-cmd/id cmd) (pl-cmd/update-state! cmd :cancel))
    (pl-cmd/cancel-command cmd)
    ))

(defn plant-commands-from-rmq [command metadata]
  ;(println "\nGot commands for plant:" (:routing-key metadata))
  ;(pprint command)
  (cond (= :start (:state command))
        (handle-command-start command (:routing-key metadata))

        (= :cancel (:state command))
        (handle-command-cancel command)

        :else
        (do (println "Unknown command state:" (:state command))
            (pi/failed (:plant-interface @state) (:routing-key metadata) (:id command)
                       {:command-status (str "Unkown command state: " (:state command))} nil))))

(defn setup-commands-handler [queue handler plant-id]
  (if (:commands-queue @state)
    (async/close! (:commands-queue @state)))

  (async/go-loop [msg (async/<! queue)]
    (if-not msg
      (println "Blocking Queue is closed for receiving commands from:" plant-id)
      (do
        (handler (:data msg) (:metadata msg))
        (recur (async/<! queue)))))

  (update-state! :commands-queue queue))

(defn handle-rmq-message [data metadata]
  (async/>!! (:commands-queue @state) {:metadata metadata :data (tpn-json/map-from-json-str (String. data "UTF-8"))}))

(defn setup-plant-commands-subscriptions [routing-key exchange channel]
  (setup-commands-handler (async/chan) plant-commands-from-rmq routing-key)

  (let [rmq-subscription (rmq/make-subscription routing-key (fn [_ metadata data]
                                                              (handle-rmq-message data metadata)
                                                              ) channel exchange)]
    (update-state! {:plant-commands-subscription rmq-subscription})))

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default "pamela"]
                  ["-i" "--plant-id plant-id" "Plant Id of this instance of the plant" :default "plant"]
                  ["-?" "--help"]])

(defn usage [options-summary]
  (->> ["Shell exec Plant"
        ""
        "Usage: java -jar shell-plant-0.1.2-SNAPSHOT.jar [options]"
        ""
        "Options:"
        options-summary
        ""
        ]
       (string/join \newline)))

(defn exit []
  (when-not repl (System/exit 0)))

(defn cancel-subscription []
  (rmq/cancel-subscription (get-in @state [:plant-commands-subscription :consumer-tag])
                           (get-in @state [:plant-interface :channel]))
  (swap! state dissoc :plant-commands-subscription))

(defn scp-send-test []
  (pl-scp/test-send-file)
  (println "Exiting ...")
  (exit))

(defn -main [& args]
  (let [parsed (cli/parse-opts args cli-options)
        options (:options parsed)
        report-usage (or (:errors parsed) (:help (:options parsed)))
        ]
    #_(pprint parsed)

    (when report-usage
      (println (usage (:summary parsed)))
      (when (:errors parsed) (println (string/join \newline (:errors parsed))))
      (exit))

    (when (:plant-commands-subscription @state)
      (println "Plant Commands subscription is already setup. Cancelling")
      (cancel-subscription)
      #_(pi/close-connection)                           ; FIXME
      )

    (when-not report-usage
      (let [exchange (:exchange options)
            host (:host options)
            port (:port options)
            plant-id (:plant-id options)
            plant-interface (pl-con/make-plant-connection exchange {:host host :port port})
            ]
        (update-state! {:plant-interface plant-interface})
        (pprint @state)
        (setup-plant-commands-subscriptions plant-id (:exchange plant-interface) (:channel plant-interface))))))

(defn show-command-state []
  (map (fn [[k v]]
         (println "Got" k (type v))
         (when (:state v)
           {k (merge {:command (get-in (k @state) [:command-map :args])} (select-keys v [:state :exit-value]))}
           )
         ) @state))

