;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant-examples.exec
  "Supporting functions for forking a process from Java"
  (:require [pamela.tools.plant-examples.command :refer :all]
            [pamela.tools.utils.java-exec :as jexec])
  (:import (java.util.concurrent TimeUnit)))

(def debug true)

(defrecord cmd-exec [state finish-handler command-map]
  commandI
  (id [_] (:id command-map))
  (command [_]
    (str "exec: " (:args command-map)))
  (get-state [obj]
    (:state obj))
  (update-state! [obj in-state]
    (merge obj {:state in-state}))
  (start-command [obj]
    (merge obj (update-state! obj :started) (jexec/start-process (:id command-map) (:args command-map) finish-handler)))
  (cancel-command [obj]
    (jexec/cancel-process (:started-process obj))
    (merge obj (update-state! obj :cancelled))))

(defn make-exec-command [finish-handler command-map]
  "finish-handler must take three args [id exitValue other-m]"
  (->cmd-exec :start finish-handler command-map))

;; Functions to test from repl
(defn start-and-check [args]
  (let [cmd (make-exec-command (fn [id exitValue other-m]
                                 (println "Command finished" id "with exit-value" exitValue "other-info" other-m))
                               {:id   "i-123"
                                :args args})

        cmd (start-command cmd)
        ]
    (println "Process started")
    (jexec/print-process-info (:started-process cmd))
    (println "Sleep 2 sec")
    (Thread/sleep 2000)
    (jexec/print-process-info (:started-process cmd))
    (println "Waiting for process to finish or timeout 2")
    (. (jexec/make-java-process (:started-process cmd)) waitFor 2 TimeUnit/SECONDS)
    (jexec/print-process-info (:started-process cmd))

    cmd))

(defn start-and-cancel [args]
  (let [cmd (make-exec-command (fn [id exitValue other-m]
                                 (println "Command finished" id "with exit-value" exitValue "other-info" other-m))
                               {:id   "i-123"
                                :args args})
        cmd (start-command cmd)
        jprocess (jexec/make-java-process (:started-process cmd))
        ]
    (println "Process started")
    (jexec/print-process-info (:started-process cmd))
    (println "Waiting for process to finish or timeout 2")
    (when-not (. jprocess waitFor 2 TimeUnit/SECONDS)
      (println "Process still going on")
      (println "Will destroy process gracefully and then forcefully")
      (cancel-command cmd)
      #_(ProcessUtil/destroyGracefullyOrForcefullyAndWait jprocess 2 TimeUnit/SECONDS 2 TimeUnit/SECONDS))
    (jexec/print-process-info (:started-process cmd))))
