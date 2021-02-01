;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.rmq-logger.log-player
  (:gen-class)
  (:require [pamela.tools.utils.rabbitmq :as rmq]
            [pamela.tools.utils.util :as util]

            [clojure.pprint :refer :all]
            [clojure.tools.cli :as cli]
            [clojure.data.json :as json]
            [clojure.string :as str])

  (:import (java.util Date Timer TimerTask)))

; Python RMQ player
;  * Cannot keep up with thousands of events over extended periods of time
;    * Delayed publishing of events instead of real time events
;    * No clear lib available for scheduling real time events

;; Modus Operandi
; Read from RMQ Log files into memory
; store events ordered by time
; schedule events to be fired at absolute time, after a delay Xmillis
; One timer for all events to be fired at time T

; Note timer is single threaded. i.e if the timer thread is blocked, it won't fire next thread until released. This will
; cause some tightly spaced events to delayed in dispatching only if there are 1000s of them. We expect few, maybe 10 events to be
; fired at exactly same time T  but not 1000s

(defonce rmq (atom {}))
(defonce start-delay 1000)
(defonce dispatch-timer (Timer. "RMQ Event Dispatcher" true))
(defonce timer-latency (atom []))
(defonce publish-adapter nil)
(def default-rkey "#")
(def speedup 10)
(def num-lines nil)                                         ;nil implies all lines
(def dispatch-duration-expected 0)
(defonce events-count 0)                                    ;Events actually dispatched
(def events-scheduled 0)
(def repl true)
(def cached-events [])

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default "tpn-updates"]
                  ["-r" "--routing-key rkey" "Default routing-key is: #, when message itself does not has it" :default "#"]
                  ["-s" "--speedup speed" "Events will be dispatched `speedup` times faster" :default speedup :parse-fn #(Double/parseDouble %)]
                  ["-l" "--num-lines Nlines" "Number of lines to dispatch" :parse-fn #(Integer/parseInt %)]
                  ["-c" "--simulate-clock Frequency" "Will publish clock messages to rkey clock at given frequency" :parse-fn #(Integer/parseInt %) :default 0]
                  ["-?" "--help" "Print this help" :default nil]])

(defn set-speedup [val]
  (if val
    (def speedup val)))

(defn get-speedup []
  speedup)

(defn set-num-lines [val]
  (def num-lines val))

(defn set-repl [val]
  (def repl val))

(defn get-channel []
  (:channel @rmq))

(defn get-exchange []
  (:exchange @rmq))

(defn get-pending-events []
  (- events-scheduled events-count))

(defn get-cached-events []
  cached-events)

(defn set-publish-adapter [a-fn]
  (def publish-adapter a-fn))

(defn make-java-date
  "Returns java date object initialized with millis since unix epoc"
  [millis]
  (Date. millis))

(defn make-timer-task [clj-fn]
  (proxy [TimerTask] []
    (run []
      #_(println "run 1")
      (clj-fn))))

(defn get-time-for-line [line]
  (let [time-begin (str/index-of line ",")
        time-end (str/index-of line "," (inc time-begin))
        time (read-string (subs line (inc time-begin) time-end))]
    time))

(defn make-event [line]
  ;(println "time:" (get-time-for-line line))
  ;(println "map:")
  ;(pprint (map-from-json-str (util/get-everything-after 2 "," line)) )
  (let [time (get-time-for-line line)
        data (util/map-from-json-str (util/get-everything-after 2 "," line))]
    (if (and (map? data) (number? time))
      [time data]
      (util/to-std-err (println "Bad data\ntime:" time "\ndata" data)))))

(defn add-event-to-map [amap time data]
  (update amap time (fn [old-val]
                      (if (vector? old-val)
                        (conj old-val data)
                        [data]))))

(defn add-event [amap line]
  (let [[time data] (make-event line)]
    (if (and time data)
      (try
        ;(println time data)
        (add-event-to-map amap time data)
        (catch Exception e
          (println "add-event error adding line\n" line)
          (println (.getMessage e))))

      amap)))

(defn get-routing-key [data]
  (if-let [rkey (or (:received-routing-key data) (:routing-key data))]
    rkey
    default-rkey))

(defn publish-event [data]
  (def events-count (+ events-count (count data)))
  (let [data (if publish-adapter (publish-adapter data)
                                 data)]
    (doseq [event data]
      (rmq/publish-object (-> event
                              (dissoc :received-routing-key)
                              (dissoc :exchange))
                          (get-routing-key event) (:channel @rmq) (or (:exchange data) (:exchange @rmq)))))



  #_(println data))

(defn log-timer-latency
  "time is normalized time at which the event should be fired
   delta is difference between the firing-time and expected-to-fire-time
   now is absolute time at which event is fired"
  [time delta now]
  (swap! timer-latency conj [time delta now]))

(defn print-timer-latencies []
  ;(pprint @timer-latency)
  (if-let [deltas (map second @timer-latency)]
    (let [dispatch-duration (- (nth (last @timer-latency) 2)
                               (nth (first @timer-latency) 2))]
      (println "Events to be dispatched:" events-scheduled)
      (println "Events dispatched" events-count ", pending events:" (get-pending-events))
      (println "Event Dispatch latency min and max in millis:" (apply min deltas) "," (apply max deltas))
      (println "Dispatch duration in millis" dispatch-duration)
      (println "Dispatch duration actual - expected in millis:" (- dispatch-duration dispatch-duration-expected) "\n"))
    (println "No timer latency: " @timer-latency))

  ; Detect all events that need to be dispatched have indeed been dispatched and exit
  (when (not= events-scheduled events-count)
    (println "Pending events" (- events-scheduled events-count))
    (.schedule dispatch-timer (make-timer-task #(print-timer-latencies))
               (make-java-date (+ (System/currentTimeMillis) 1000))))

  (if (and (= events-scheduled events-count) (not repl))
    (do (println "Exiting")
        (System/exit 0))
    (cond repl
          (println "In repl. Not exiting")
          (= events-scheduled events-count)
          (println "= events-scheduled events-dispatched" events-scheduled events-count))))


(defn add-clock-events [events start-time end-time frequency]
  ;(println "add-clock-events" (count events) start-time end-time frequency)
  ;(pprint events)
  (if (= 0 frequency)
    events
    (let [interval (long (* 1000 (float (/ 1 frequency))))
          clks (range start-time (+ end-time interval) interval)]
      (println "Adding clock events\nCount:" (count clks) "Frequency:" frequency "Interval:" interval)
      (reduce (fn [res tim]
                (add-event-to-map res tim {:app-id :log-player :routing-key "clock" :timestamp tim}))
              events
              clks))))

(defn schedule-events [file clock-events]
  (println "\nFrom file:" file)
  (println "Speedup:" speedup)
  (reset! timer-latency [])
  (let [lines (util/read-lines file)
        ;lines (take 10 lines)
        lines (if (nil? num-lines)
                lines
                (take num-lines lines))
        events (reduce (fn [res line]
                         (add-event res line)) (sorted-map) lines)
        kees (keys events)
        ;_ (println kees)
        start-time (apply min kees)
        end-time (apply max kees)
        events (add-clock-events events start-time end-time clock-events)
        ; Normalize absolute time to start time
        events (reduce (fn [res [abs-time data]]
                         (update res (long (/ (- abs-time start-time) speedup))
                                 (fn [old-value]
                                   (if (vector? old-value)
                                     (into old-value data)
                                     data))))
                       (sorted-map) events)
        abs-start (+ start-delay (System/currentTimeMillis))
        abs-end (+ abs-start (first (last events)))
        ;_ (println "abs time" abs-start abs-end (- abs-end abs-start))
        print-stats-time (make-java-date (+ abs-end 1000))]
    (def cached-events events)
    (def dispatch-duration-expected (- abs-end abs-start))
    (println "\nAbsolute event dispatch time" (make-java-date abs-start))
    (println "Absolute last event dispatch time" (make-java-date abs-end))
    (println "\nFirst and last event time in mills: [" (first (first events)) (first (last events)) "]")
    (println "Number of events:" (count lines))
    (println "Number of timers scheduled:" (count events))
    (def events-count 0)
    (def events-scheduled 0)
    (doseq [[time data] events]
      ;(println time "->" data)
      (def events-scheduled (+ events-scheduled (count data)))
      (let [real-time (+ abs-start time)]
        (.schedule dispatch-timer
                   (make-timer-task (fn []
                                      ;(println "Firing events at " time ", delta" (- (System/currentTimeMillis) real-time))
                                      (let [now (System/currentTimeMillis)]
                                        (publish-event data)
                                        (log-timer-latency time (- now real-time) now))))


                   (make-java-date real-time))))

    (.schedule dispatch-timer (make-timer-task #(print-timer-latencies))
               print-stats-time)))


(defn setup-rmq [host port exchange rkey]
  (def default-rkey rkey)
  ;(println "RMQ Config" host port exchange rkey)
  (when (> (count @rmq) 0)
    (println "RMQ is already setup. Closing open objects")
    (rmq/close-all @rmq))
  (let [conn (rmq/make-channel exchange {:host host :port port})]
    (if conn (swap! rmq conj conn))))

(defn go [file host port exchange rkey clock-events]
  (if-not (setup-rmq host port exchange rkey)
    (System/exit 1))
  (println "Starting events dispatch in " (float (/ start-delay 1000)) "secs")
  (try
    (schedule-events file clock-events)
    (catch Exception e
      (System/exit 1))))

(defn play-and-wait "Synchronous function that blocks the calling thread until all messages have been played."
  [file host port exchange rkey clock-events]
  ; do not exit when this function is called.
  (def repl true)
  (def done false)
  (go file host port exchange rkey clock-events)
  (while (not done)
    (def done (= 0 (get-pending-events)))
    (Thread/sleep 1000))
  (println "Done play-and-wait"))

(defn -main [& args]
  (def repl false)
  (println "Realtime RMQ log player")
  ;(println "args" (count args))
  ;(pprint args)
  (let [parsed (cli/parse-opts args cli-options)
        {help         :help
         host         :host
         port         :port
         exchange     :exchange
         rkey         :routing-key
         speed        :speedup
         nlines       :num-lines
         clock-events :simulate-clock} (:options parsed)
        [file] (:arguments parsed)]
    ;(println "\ncommand line options:")
    (pprint (:options parsed))
    ;(println "\ncommand line args:")
    (pprint (:arguments parsed))
    (println)
    (def speedup speed)
    (def num-lines nlines)
    (if (or help (not file))
      (do (println "Usage: pamela.tools.rmq-logger.log-player options raw-event-data-file\n where options are:")
          (println (:summary parsed)))
      (go file host port exchange rkey clock-events))))

(defn write-sorted-data
  "To sort data from a csv file and write it back"
  [from-file to-file]
  (let [lines (util/read-lines from-file)
        ;lines (take 10 lines)
        events (reduce (fn [res line]
                         (add-event res line)) (sorted-map) lines)]

    (with-open [w (clojure.java.io/writer to-file)]

      #_(.write w (str "yel"))
      (doseq [[time datav] events]
        (doseq [event datav]
          (.write w (str "raw-data," time "," (clojure.data.json/write-str event) "\n")))))
    #_(pprint events)))

;;; scratch
(defonce test-data [])

(defn read-log-file [fname]
  (reduce (fn [res line]
            (let [[_ data] (make-event line)]
              (conj res data)))
          [] (util/read-lines fname)))

;;load only events from csv file into test-data
(defn load-test-data [fname]
  (let [lines (util/read-lines fname)
        data (reduce (fn [res line]
                       (let [[_ data] (make-event line)]
                         (conj res data)))
                     [] lines)]

    (def test-data data)))