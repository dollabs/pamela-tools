;
; The software contained herein is proprietary and
; a confidential trade secret of Dynamic Object Language Labs Inc.
; Copyright (c) 2020.  All rights reserved.
;

; Interface to wrap timeout / clock related functionality so that we can use
; either real time clock as provided by the system or clock as provided by messages published to /clock routing-key

(ns pamela.tools.utils.timer
  ^{:doc "Namespace to wrap timer functionality"}
  (:require [pamela.tools.utils.util :as pt-util]
            [ruiyun.tools.timer :as timer]
            [clojure.core.async :as async])

  (:import (java.time Instant)))

(defonce timer (timer/deamon-timer "Pamela tools timer"))
(def use-sim-time false)
(defonce clock (atom 0))

(defn reset-timer []
  (timer/cancel! timer)
  (def timer (timer/deamon-timer "Clock generator")))

(defn set-use-sim-time [val]
  ;{:pre [((ol))]}
  (println "Pamela Tools timer use-sim-clock" val)
  (if (and (not (nil? val) ) (or (true? val) (false? val)))
    (def use-sim-time val)
    (pt-util/to-std-err (println "Bad value for set-use-sim-time" val)))
  use-sim-time)

(defn get-use-sim-time []
  use-sim-time)

; we will be checking for fired timers more frequently than scheduling a timer
; so optimize for faster checks using vector TODO only necessary if scheduling tons of timers

; each element of the vector is [absolute-timestamp [fns to be called when the absolute-time has expired]]
(defonce call-backs (agent []))

(defn reset-call-back-agent []
  (restart-agent call-backs @call-backs))

(defn reset-call-back-state []
  (send call-backs (fn [_]
                     [])))

(defn- insert-real [cbs time fn]
  ;(println "cbs" cbs time fn)
  (if (nil? cbs)
    (conj [] [time fn])
    (conj cbs [time fn])))

(defn- insert [time fn]
  (send call-backs insert-real time fn))

(defn- fire-timers [ts]
  (doseq [[tim afn] ts]
    (println "Firing timer for T="tim (.getName (Thread/currentThread)))
    (try (afn)
         (catch Exception e (pt-util/to-std-err
                              (println "dispatching timer" tim "caught exception: " (.getMessage e)))))))

(defn- process-timers [cbs cur-time]
  ;(println "Processing timers for time" cur-time)
  (let [to-fire (atom [])
        pending-timers (reduce (fn [res x]
                                 (if (>= cur-time (first x))
                                   (do (swap! to-fire conj x)
                                       res)
                                   (conj res x)))
                               [] cbs)]
    ;(println "call-backs" cbs)
    ;(println "to-fire" to-fire)
    ;(println "pending" pending-timers)
    (async/thread (fire-timers @to-fire))
    pending-timers))


(defn update-clock
  "To be called whenever we want to update internal clock
  Will trigger any timers that are fired.
  Each timer will be fired using core.async/thread function"
  [cval]
  {:pre [(not (nil? cval))]}
  (if (> @clock cval)
    (pt-util/to-std-err (println "update-clock value is in past:" cval "current clock:" @clock "delta(ms): " (- @clock cval))))

  (do (reset! clock cval)
      (if (get-use-sim-time)
        (send call-backs process-timers cval))))

(defn get-unix-time []
  (if use-sim-time
    @clock
    (System/currentTimeMillis)))

(defn schedule-task
  "schedule a call back task to be fired after given delay"
  [fn delay & [wrt-time]]
  {:pre [(not (nil? fn)) (not (nil? delay))]}

  (when (not use-sim-time)
    #_(println "Pamela timer using real time")
    (if (nil? wrt-time)
      (timer/run-task! fn :delay delay :by timer)
      (let [at-time (+ delay wrt-time)
            now     (get-unix-time)
            del     (if (> at-time now)
                      (- at-time now)
                      0)]
        (timer/run-task! fn :delay del :by timer))))

  (when use-sim-time
    #_(println "Pamela timer using sim clock time")
    (if (nil? wrt-time)
      (insert (+ (get-unix-time) delay) fn)
      (insert (+ wrt-time delay) fn))))

(defn getTimeInSeconds
  ([]
   (/ (get-unix-time) 1000))
  ([m]
   (if (and (contains? m :time)
            (not (nil? (:time m))))
     (:time m)
     (getTimeInSeconds))))

(defn make-instant [& [millis]]
  (if millis (Instant/ofEpochMilli (long millis))
             #_(Instant/now (get-unix-time))))

(defn make-instant-seconds [seconds]
  (make-instant (* 1000 seconds)))

(defn- schedule-test []
  (doseq [t (range 1 10)]
    ;(println "t" t)
    (schedule-task (fn []
                     (println "tst timer" t)) (* 100 t))))

(defn as-str []
  (str (Instant/ofEpochMilli (get-unix-time))))

(defn print-call-backs []
  (doseq [x @call-backs]
    (println (make-instant (first x)))))