;
; The software contained herein is proprietary and
; a confidential trade secret of Dynamic Object Language Labs Inc.
; Copyright (c) 2012.  All rights reserved.
;

; Application to simulate plant methods

(ns pamela.tools.plant.plant-sim
  ^{:doc "An application that simulates execution of activities."}
  (:gen-class)
  (:require [pamela.tools.utils.tpn-json :as tpn-json]
            [pamela.tools.utils.util :as pt-utils]
            [pamela.tools.utils.rabbitmq :as rmq]
            [pamela.tools.utils.timer :as pt-timer]
            [clojure.pprint :refer :all]
            [clojure.tools.cli :as cli]

            [clojure.string :as string]
            [clojure.data.csv :as csv]))

(def repl true)
(defonce connection-info nil)
(defonce networks (atom {}))
(defonce activity-info {})
(defonce command-state (atom {}))                           ;to keep track of runtime state of the plant-command

; Time for which simulator will pretend to execute activity
(def activity-time 5)                                       ; unit is seconds

; When true, activity temporal bounds will be used to execute activity
; Temporal bounds will override activity-time.
; However in case of, infinity, activity-time will be used.
; Applicable to classic mode only
(def use-temporal-bounds false)

(def classic-mode false)
(def fail false)
(def fail-randomly false)
(def fail-freq 3)                                           ; randomly fail once every 3 times

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default "tpn-updates"]
                  ["-r" "--routing-key" "Routing Key for receiving commands from dispatcher" :default "tpn.activity.negotiation"]
                  [nil "--plant-ids plant-ids,as-csv" "Comma separated list of plant ids" :default "plant"]
                  ["-d" "--activity-time activity-time" "Time for which activity will run (in seconds)" :default 5 :parse-fn #(Float/parseFloat %)]
                  ["-t" "--use-temporal-bounds" "When in classic mode, activity execution time will be upper-bound of temporal bounds(in seconds)" :default false]
                  ["-c" "--in-classic-mode" "When true, Sim will respond to activity start/negotiation messages (Old style message passing).
                  Otherwise operate in plant mode and respond to plant :start and :cancel messages"
                   :default false]
                  ["-a" "--activity-info a-info.json" "JSON File containing activity execution time. Will override default and -d value if specified\n Ex: {:act-123 {:execution-time 11} Note: :execution-time is in seconds"]
                  [nil "--fail" "Always fail activities" :default false]
                  [nil "--fail-randomly" "Randomly fail activities" :default false]
                  [nil "--simulate-clock" "Will listen for clock messages on rkey `clock` and use the clock for timeouts only!" :default false]
                  ["-?" "--help"]
                  ])

(defn publish [id state net-id routing-key]
  (println "publish " id state net-id routing-key)
  (rmq/publish-object {:network-id net-id
                       id          {:uid              id
                                    :tpn-object-state state}}
                      routing-key (:channel connection-info) (:exchange connection-info)))

(defn get-activity-execution-time
  "Return activity execution time in the following order of precedence
  1. from activity-info file
  2. upper temporal bounds specified in the activity
  3. default activity-time"                                 ;Note -d option overrides default activity time
  [act-id netid]
  (let [time-precedence [activity-time]
        network         (get @networks netid)
        constraints     (filter (fn [constraint]
                                  (= :temporal-constraint (:tpn-type constraint))
                                  ) (pt-utils/get-constraints act-id network))
        ub              (if (and use-temporal-bounds (pos? (count constraints)))
                          (second (:value (first constraints))))
        time-precedence (if ub
                          (conj time-precedence ub)
                          time-precedence)

        time-precedence (if (contains? activity-info act-id)
                          (conj time-precedence (get-in activity-info [act-id :execution-time]))
                          time-precedence)]
    (println "get-activity-execution-time" act-id time-precedence)
    (* 1000 (last time-precedence))))

(defn rmq-data-to-clj [data]
  (tpn-json/map-from-json-str (String. data "UTF-8")))

(defn handle-activity-finished [act-id network-id]
  (cond (true? fail)
        (publish act-id :failed network-id "tpn.activity.finished")

        (true? fail-randomly)
        (if (zero? (mod (rand-int fail-freq) fail-freq))
          (publish act-id :failed network-id "tpn.activity.finished")
          (publish act-id :finished network-id "tpn.activity.finished"))

        :else
        (publish act-id :finished network-id "tpn.activity.finished")))

(defn incoming-msgs [_ _ data]
  (let [st (String. data "UTF-8")
        m  (tpn-json/map-from-json-str st)]
    (doseq [[k v] m]
      (when (instance? clojure.lang.IPersistentMap v)
        (let [before (pt-timer/getTimeInSeconds)]
          (println "Got activity start" k (:network-id m))
          (pt-timer/schedule-task #(publish k :active (:network-id m) "tpn.activity.active") 100)
          (pt-timer/schedule-task (fn []
                                    (println "finished " k "in time" (float (- (pt-timer/getTimeInSeconds) before)))
                                    (handle-activity-finished k (:network-id m)))
                                  (get-activity-execution-time k (:network-id m))))))))

(defn new-tpn [_ _ data]
  (let [strng (String. data "UTF-8")
        m     (tpn-json/map-from-json-str strng)]
    (println "Got new tpn" (:network-id m))

    (swap! networks assoc (:network-id m) m)
    (println)))

(defn publish-command-state [id plant-id state timestamp reason]
  (rmq/publish-object (merge {:plant-id plant-id :id id :state state :timestamp timestamp} reason) "observations"
                      (:channel connection-info) (:exchange connection-info)))


(defn finish-command [msg plant-id state]
  (when (contains? @command-state (:id msg))
    (publish-command-state (:id msg) plant-id :finished (System/currentTimeMillis)
                           {:reason {:finish-state state
                                     :str          (str "Finished simulating "
                                                        "command: " (:pclass msg) "." (:pmethod msg)
                                                        " " (:args msg)
                                                        )}})
    (swap! command-state dissoc (:id msg))))

(defn handle-plant-finished [msg plant-id]
  (cond (true? fail)
        (finish-command msg plant-id :failed)

        (true? fail-randomly)
        (if (zero? (mod (rand-int fail-freq) fail-freq))
          (finish-command msg plant-id :failed)
          (finish-command msg plant-id :success))

        :else
        (finish-command msg plant-id :success)))

(defn start-command [msg plant-id]
  (println "Plant command: " (:function-name msg) (:args msg) "for plant-id" plant-id)
  (let [id (:id msg)]
    (when-not (contains? @command-state id)
      (swap! command-state assoc id []))

    (swap! command-state update id conj :start)
    ; We cannot cancel a scheduled task because timer utility does not has an interface for it.
    ; so we will remove command-id in command-state as an indicator if the task is active or not.
    (pt-timer/schedule-task (fn []
                              (publish-command-state id plant-id :started (System/currentTimeMillis) nil)
                              (swap! command-state update id conj :started))
                            500)
    ; The plant does not know anything about the bounds. So activity sim time is activity-time
    (pt-timer/schedule-task #(handle-plant-finished msg plant-id) (* 1000 activity-time))))

(defn cancel-command [msg plant-id]
  (finish-command msg plant-id :cancelled))

(defn plant-commands [_ metadata data]
  ;(println "Got plant command")
  #_(println "metadata" metadata)
  (let [msg (rmq-data-to-clj data)]
    ;(pprint msg)

    (cond (= :start (:state msg))
          (start-command msg (:routing-key metadata))
          (= :cancel (:state msg))
          (cancel-command msg (:routing-key metadata))
          (nil? (:state msg))
          (do
            #_(println "non plant message . ignore"))
          :else
          (do
            #_(println "I cannot handle :state" (:state msg) "for id" (:id msg)))
          )))

(defn update-clock [_ _ data]
  (let [msg (rmq-data-to-clj data)
        ts  (:timestamp msg)]
    (if ts (pt-timer/update-clock ts))))

(defn usage [options-summary]
  (->> ["Plant Interface Simulation Engine"
        ""
        "Usage: java -jar plant-sim-X.X.X-SNAPSHOT.jar [options]"
        ""
        "Options:"
        options-summary]
       (string/join \newline)))

(defn print-help [summary-options]
  (println (usage summary-options)))

(defn stop-plant-processing []
  (when connection-info
    (println "Stopping plant sim")
    (rmq/close-connection (:connection connection-info))))

(defn exit [code]
  (when-not repl
    (System/exit code))
  (if repl
    (println "in repl mode. Not exiting")))

(defn -main [& args]
  (let [parsed          (cli/parse-opts args cli-options)
        opts            (:options parsed)
        ;_ (pprint opts)
        help            (:help opts)
        plant-ids       (:plant-ids opts)
        plant-ids       (if plant-ids (first (csv/read-csv plant-ids)))
        conn-info       (rmq/make-channel (:exchange opts) opts)
        a-time          (:activity-time opts)
        act-info        (:activity-info opts)
        temporal-bounds (:use-temporal-bounds opts)
        fail            (:fail opts)
        failr           (:fail-randomly opts)
        sim-clock       (:simulate-clock opts)
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

    (def classic-mode (:in-classic-mode opts))

    (when (true? fail)
      (def fail true))

    (when (true? failr)
      (def fail-randomly true))

    (if classic-mode
      (println "Operating in classic mode")
      (println "Operating in plant mode"))

    (cond classic-mode
          (do
            (println "Setting up classic mode subscriptions")
            (rmq/make-subscription (:routing-key opts) incoming-msgs (:channel conn-info) (:exchange opts))
            (rmq/make-subscription "network.new" new-tpn (:channel conn-info) (:exchange opts)))

          plant-ids
          (do (println "subscribing plant-ids:" plant-ids)
              (doseq [plant-id plant-ids]
                (rmq/make-subscription plant-id plant-commands (:channel conn-info) (:exchange opts))))
          :else
          (do
            (println "subscribing all plant messages:")
            (rmq/make-subscription nil plant-commands (:channel conn-info) (:exchange opts))))

    (def activity-time a-time)
    (println "Each activity / plant-command will run for:" activity-time "seconds")

    (def use-temporal-bounds temporal-bounds)
    (println "Using temporal bounds:" use-temporal-bounds)

    (when act-info
      (println "Reading activity info from" act-info)
      (def activity-info (tpn-json/from-file act-info))
      (println "Activity info")
      (pprint activity-info))

    (pt-timer/set-use-sim-time sim-clock)
    (if sim-clock
      (rmq/make-subscription "clock" update-clock (:channel conn-info) (:exchange opts)))

    ; Cleanup previous connection
    (stop-plant-processing)

    (def connection-info conn-info)
    (println "App State")
    (println "----------")
    (clojure.pprint/pprint connection-info)
    (println "----------")))