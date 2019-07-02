;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns plant.core-test
  (:require [pamela.tools.utils.util :as tu]
            [clojure.pprint :refer :all]
            [automat.core :as a]
            ))

(def method-call-fsm (a/or [:start :started (a/* :observations) :finished]
                           [:start :started (a/* :observations) :cancel :cancelled :finished]
                           [:start :finished]               ;Case when plant is unable to start the command
                           ))
(def compiled-fsm (a/compile method-call-fsm))

(def state-checker-internal (partial a/advance compiled-fsm))
(defn state-checker [current-state value]
  (state-checker-internal current-state value :error))

(defn check-states [states]
  (reduce state-checker nil states))

(defn get-data-val [key msg]
  (get-in msg [:data key]))

(defn check-plant-state-transitions [p-msgs]
  "p-msgs is a list of messages for a method invocation"

  #_(println "----" (key p-msgs))

  (let [states (map (fn [msg]
                      (get-data-val :state msg)) (val p-msgs))
        checked (check-states states)]
    ;(println "Checked")
    ;(pprint checked)
    (cond (true? (:accepted? checked))
          true

          (= :error checked)
          (do (println "Error for method id" (key p-msgs))
              (println "Got States in order: " states)
              (pprint p-msgs)
              false)

          :else
          (do (println "UnKnown check state")
              nil))))

; TODO Work in progress
;; Test to read a plant execution trace and very all messages are fired by all involved
;; components in the correct order

; Assume input file is a CSV file as follows.
; ignore something, time-as-long-millis, JSON string
; Note rmq-logger listens on RMQ and spits messages  in above format.
; You will need tp grep on 'ignore something' to extract lines for this test

(defn read-csv-file [name]
  (println "Reading trace messages from:" name)
  (let [lines (tu/read-lines name)
        data (map (fn [line]
                    (tu/parse-rmq-logger-json-line line)) lines)
        plant-msgs (group-by (fn [msg]
                               (get-in msg [:data :id])
                               ) (filter (fn [msg]
                                           (get-in msg [:data :id])) data))

        ]
    #_(pprint data)
    ;(pprint plant-msgs)
    (doseq [p-msgs plant-msgs]
      ;(println x)
      (check-plant-state-transitions p-msgs))))

