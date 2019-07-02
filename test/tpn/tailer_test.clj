;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns tpn.tailer-test
  (:require [pamela.tools.utils.rabbitmq :as rmq]
            [pamela.tools.utils.tpn-json :as tjson]
            [pamela.tools.utils.util :as tu]
            [clojure.test :refer :all]
            [clojure.string :as str]
            ))

(defonce rmq nil)

(defn close-rmq []
  (when rmq
    (rmq/close-connection (:connection rmq))
    (def rmq nil)))

(defn test-publisher [file]
  (if rmq (println "rmq is valid !"))
  (when-not rmq
    (def rmq (rmq/make-channel "tpn-updates" {})))

  ; Assume each line of form "raw-data, long-time-stamp, json-string
  ; add test-send-time to the message and publish in a tight loop
  (let [lines (tu/read-lines file)
        lines (remove nil? (map (fn [line]
                                  (tu/get-everything-after 2 "," line)) lines))
        lines (map (fn [line]
                     (tjson/map-from-json-str line)
                     ) lines)
        {channel :channel exchange :exchange} rmq
        ]
    (println "got lines" (count lines))
    (time (doseq [msg lines]
            (rmq/publish-object (conj msg {:test-send-time (System/currentTimeMillis)}) "test-msgs-prakash" channel exchange)))

    (println "Done publishing")
    ))

(defn get-latency-for-line [line]
  (let [time-begin (str/index-of line ",")
        time-end (str/index-of line "," (inc time-begin))
        time (read-string (subs line (inc time-begin) time-end))
        js-str (subs line (inc time-end))
        m (tjson/map-from-json-str js-str)]
    (if (contains? m :test-send-time)
      (- time (:test-send-time m))
      (do
        (println "no send-time\n" line)
        nil))))

(defn check-publish-latency [file]
  (let [lines (tu/read-lines file)
        ;lines (take 2 lines)
        lats (into [] (map get-latency-for-line lines))]
    (println "In millis, min, max and average" (apply min lats) "," (apply max lats) "," (float (/ (apply + lats) (count lats))))
    ))

; Run test-publisher as below with a very large file. Output will say how long it took to send
; all messages in a tight loop.
;
; RMQ Tailer will receive all the messages with received timestamp and test-send-time
; receive-timestamp - test-send-time is the latency, currently observed to be 2-3 millis  for the first and last message
;
; (tpn.rmq-logger-test/test-publisher "/Users/prakash/projects/pac2man/logs-2017-1124-2047/deduce-raw-data.txt")
; rmq is valid !
; got lines 157718
; "Elapsed time: 10014.611648 msecs"
; Done publishing

; (check-publish-latency "test/tpn/data/raw-data-with-test-timestamps.txt")
; In millis, min, max and average 0 , 60 , 1.0865659
