;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.dispatcher_manager.core
  "Plant Implementation to dispatch TPNs"
  (:require [pamela.tools.utils.java-exec :as jexec]
            [pamela.tools.utils.tpn-json :as tpn_json]
            [pamela.tools.utils.util :as tpn_util]
            [pamela.tools.utils.rabbitmq :as rmq]
            [pamela.tools.dispatcher.dispatch-app :as dispatch_app]
            [pamela.tools.plant.connection :as pc]
            [pamela.tools.plant.interface :as plant]
            [clojure.pprint :refer :all]
            [clojure.tools.cli :as cli]
            [clojure.string :as string])
  (:import (java.io File))
  (:gen-class))


(defonce state (atom {}))
(defonce rmq nil)

(def default-plant-id "dispatcher-manager")                 ;Will listen on this routing key for incoming tpns to be dispatched
(def dispatch-command "dispatch_tpn")

; https://stackoverflow.com/questions/636367/executing-a-java-application-in-a-separate-process
(def javaHome (System/getProperty "java.home"))
(def javaBin (str javaHome (File/separator) "bin" (File/separator) "java"))
(def classpath (System/getProperty "java.class.path"))

(defn reset-state! []
  (reset! state {}))

(defn init-plant-interface [& [exchange host port]]
  (if-not rmq
    (def rmq (pc/make-plant-connection (or exchange dispatch_app/default-exchange)
                                       {:host (or host rmq/default-host)
                                        :port (or port rmq/default-port)}))
    (println "Rabbit Plant Connection is already setup")))

(defn done-plant-interface []
  (plant/close-connection rmq)
  (def rmq nil))

(defn show-command-state [msg-id]
  (println "command state id" msg-id (get-in @state [msg-id :state])))

(defn update-command-state!
  "Keep track of state of the command as it progresses through various stages of execution"
  [msg-id current-state]
  ;(show-command-state msg-id)
  (swap! state update-in [msg-id :state]
         (fn [prev-state-vec]
           (let [prev (if-not prev-state-vec
                        []
                        prev-state-vec)]
             (conj prev current-state)))))

(defn update-state-start!
  "Save start msg indexed by its id along with process-info"
  [msg process-info]
  (when (contains? @state (:id msg))
    (println "Warning state already contains info about msg id" (:id msg) ". Will overwrite"))
  (swap! state update-in [(:id msg)] (fn [prev]
                                       (merge prev process-info {:plant-id (:plant-id msg)})))
  (update-command-state! (:id msg) (:state msg))
  (show-command-state (:id msg)))

(defn dispatcher-finished-handler [id exit-code]
  ; todo assuming plant id. fixme
  (let [last-state (last (get-in @state [id :state]))
        finish-state (cond (= :cancel last-state)
                           :cancelled

                           (= 0 exit-code)
                           :success

                           :else
                           :failed)
        plantid (get-in @state [id :plant-id])]
    (println "Dispatcher " id "finished with exit-code" exit-code "state" finish-state "last-state" last-state)
    (update-command-state! id finish-state)
    (show-command-state id)
    (cond (= finish-state :cancelled)
          (plant/cancelled rmq plantid id {:exit-code exit-code} nil)

          (= finish-state :failed)
          (plant/failed rmq plantid id {:exit-code exit-code} nil)

          (= finish-state :success)
          (plant/finished rmq plantid id {:exit-code exit-code} nil))))

(defn start-dispatcher [msg-id command & [stdout]]
  (jexec/start-process msg-id (into [javaBin "-cp" classpath] command)
                       dispatcher-finished-handler (if stdout
                                                     nil
                                                     :temp)))

(defn create-temp-tpn-file [msg-id tpn]
  (let [tpn-file (jexec/make-tmp-file msg-id ".tpn.json")]
    (tpn_json/to-file tpn tpn-file)
    tpn-file))

(defn update-with-unique-network-id [msg-id tpn]
  (let [netid-old (:network-id tpn)
        netid (keyword (str (name netid-old) "-" msg-id))]
    (merge tpn {:network-id netid
                netid       (netid-old tpn)})))

(defn dispatch-tpn [msg-id tpn show-in-planviz]
  (println "Dispatching tpn " msg-id "show-in-planviz" show-in-planviz)
  (let [msg-id-str (name msg-id)
        tpn (if show-in-planviz
              (update-with-unique-network-id msg-id-str tpn)
              tpn)
        tpn-file (create-temp-tpn-file msg-id-str tpn)
        command ["pamela_tools.dispatcher.dispatch_app"
                 "-e" (plant/get-exchange rmq)
                 "-h" (plant/get-host rmq)
                 "-p" (plant/get-port rmq)]
        command (if-not show-in-planviz
                  (conj command "--no-tpn-publish")
                  command)
        command (conj command (.getCanonicalPath tpn-file))]
    (println "tpn-file" tpn-file)
    (println "Starting dispatcher as" command)
    (start-dispatcher msg-id command )))

(defn handle-start-msg [msg]
  (let [{msg-id                :id
         plant-id              :plant-id
         state                 :state
         command               :function-name
         [tpn show-in-planviz] :args
         } msg]
    (cond (and (= command dispatch-command))
          (do (let [proc-info (dispatch-tpn msg-id tpn show-in-planviz)]
                (update-state-start! msg proc-info)))

          :else
          (tpn_util/to-std-err
            (println "Bad message id" msg-id "state =" state "should be(" :start ")" "command = " command "should be(" dispatch-command ")"))
          )
    (plant/started rmq plant-id msg-id nil)
    (update-command-state! msg-id :started)
    (show-command-state msg-id)))

(defn handle-cancel-msg [msg]
  (let [{msg-id :id} msg
        started-process (get-in @state [msg-id :started-process])]
    (when-not started-process
      (println "Warning: handle-cancel-msg" msg-id ".started-process is nil"))
    (update-command-state! (:id msg) :cancel)
    (show-command-state (:id msg))
    (when started-process
      (println "cancelling msg with id" msg-id)
      (jexec/cancel-process started-process))))

(defn incoming-rmq-messages [msg]
  ;(pprint msg)
  (let [state (:state msg)]
    (cond (= :start state)
          (handle-start-msg msg)

          (= :cancel state)
          (handle-cancel-msg msg)

          :else
          (tpn_util/to-std-err
            (println "Bad message. state is" state "should be :start or :cancel"))
          )
    )
  )

(defn subscribe-for-commands [routing-key]
  (def rmq (plant/subscribe rmq routing-key (fn [_ _ data]
                                              (incoming-rmq-messages (tpn_json/map-from-json-str (String. data "UTF-8")))))))

(defn cancel-subscription []
  (def rmq (plant/cancel-subscription rmq)))

(defn test-publish-tpn [tpn-data & [cancel]]
  (let [tpn (first tpn-data)
        msg-id (second tpn-data)
        show-in-planviz true]
    (plant/start rmq
                 default-plant-id
                 msg-id
                 "dispatch_tpn"
                 [tpn show-in-planviz]
                 {:tpn             tpn
                  :show-in-planviz show-in-planviz}
                 nil nil)
    (when cancel
      (println "test-publish-tpn Sleeping for 10 secs")
      (Thread/sleep 10000)
      (println "test-publish-tpn sending cancel")
      (plant/cancel rmq default-plant-id msg-id nil))))

(defn test-publish-parallel [& [cancel]]
  (test-publish-tpn [(tpn_json/from-file "test/data/parallel.tpn.json") "my-parallel"] cancel))

(defn test-publish-choice [& [cancel]]
  (test-publish-tpn [(tpn_json/from-file "test/data/choice.tpn.json") "my-choice"] cancel))

(defn test-many []
  (doseq [i (range 5)]
    (println i)
    (test-publish-tpn [(tpn_json/from-file "test/data/choice.tpn.json") (str "my-choice-" i)])))

(defn check []
  (let [p-info (start-dispatcher "java-test" ["pamela_tools.dispatcher.dispatch_app"] true)] ;should only produce dispatcher help on stdout
    (jexec/wait-for-process (:started-process p-info))
    (println "Done waiting indefinitely ")))


(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default rmq/default-host]
                  ["-p" "--port rmqport" "RMQ Port" :default rmq/default-port :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default dispatch_app/default-exchange]
                  ["-k" "--subscription-key key" "Dispatch Manager plant-id" :default default-plant-id]
                  ["-?" "--help"]])

(defn usage [options-summary]
  (->> ["TPN Dispatch Manager"
        ""
        "Usage: java -jar dispatcher-manager-xxx-standalone.jar [options]"
        ""
        "Options:"
        options-summary
        ""
        ]
       (string/join \newline)))

(defn -main
  "Dispatches TPN via RMQ"
  [& args]

  (let [parsed (cli/parse-opts args cli-options)
        options (:options parsed)
        {host             :host
         port             :port
         exchange         :exchange
         subscription-key :subscription-key
         help :help} options]

    ;(swap! state assoc :cl-info parsed)
    (when help
      (println (usage (:summary parsed)))
      (System/exit 0))

    (println "Dispatcher Plant")
    (println "Connection options")
    (pprint (:options parsed))



    (swap! state assoc :options (:options parsed))
    (init-plant-interface exchange host port)
    (subscribe-for-commands subscription-key)
    (println "Waiting for '" dispatch-command "[tpn-as-json show-in-planviz(true/false)] ' commands"))
  #_(println "Dispatcher Plant -- done")                    ;Even though we return from main, rabbit event loops prevent main from exiting
  )