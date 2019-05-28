;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant-examples.scp
  "Implementation of scp function for exec-plant.pamela"
  (:require [pamela.tools.plant-examples.command :refer :all]
            [clojure.pprint :refer :all])
  (:import [net.schmizz.sshj SSHClient]
           [net.schmizz.sshj.xfer FileSystemFile]
           [java.io FileInputStream IOException]
           [net.schmizz.sshj.transport.verification OpenSSHKnownHosts]
           (clojure.lang Atom)))

(defn- make-counting-is [file ^Atom bytes-collector]
  "A FileInputStream object that keeps track of bytes read."
  (proxy [FileInputStream] [file]
    (read
      ([]
       (let [red (proxy-super read)]
         (if-not (= -1 red)
           (swap! bytes-collector inc))
         red))

      ([b]
       (let [red (proxy-super read b)]
         (if-not (= -1 red)
           (swap! bytes-collector (fn [prev]
                                    (+ prev red))))
         red))

      ([b off len]
       (let [red (proxy-super read b off len)]
         (if-not (= -1 red)
           (swap! bytes-collector (fn [prev]
                                    (+ prev red))))
         red)))))

(defn- make-counting-fsf [file]
  "Return an instance of FileSystemFile but keeps tracks of bytes read so far. Deref the returned object
  to receive references for all objects involved in the file transfer process"
  (let [bytes-sent (atom 0)
        input-streams (atom [])]

    (proxy [FileSystemFile clojure.lang.IDeref] [file]
      ;
      (getInputStream []
        ;(println "get input stream called")
        (swap! input-streams conj (make-counting-is (.getFile this) bytes-sent))
        ;(println "input streams for fsf" (count @input-streams))
        (last @input-streams))

      (deref []
        {:bytes-sent-atom    bytes-sent
         :input-streams-atom input-streams})
      )))

(defn- start-upload [id argsmap finish-handler]
  (let [ssh (doto (SSHClient.)
              (.loadKnownHosts)
              (.connect (:host argsmap))
              (.authPublickey (-> (System/getProperties) (.get "user.name"))))
        control-obj (make-counting-fsf (:fromfile argsmap))
        file-length (.getLength control-obj)
        transfer (.newSCPFileTransfer ssh)
        scp-future (future (do
                             (try
                               (.upload transfer control-obj (:tofile argsmap))
                               (catch IOException _
                                 #_(println "Got exception" (.getMessage e))))

                             (let [data-transferred @(:bytes-sent-atom @control-obj)

                                   result (- file-length data-transferred)]
                               (finish-handler id result {})
                               result)))]
    {:scp-object  control-obj :scp-result-future scp-future :socket-connection ssh
     :file-length file-length :start-time (System/currentTimeMillis)}))

(defrecord scp-upload [state finish-handler command-map]
  commandI
  (id [_] (:id command-map))
  (command [_]
    (str "scp-upload: " (:args command-map)))
  (get-state [obj]
    (:state obj))
  (update-state! [obj in-state]
    (merge obj {:state in-state}))
  (start-command [obj]
    (merge obj (update-state! obj :started)
           (start-upload (:id command-map) (:argsmap command-map) finish-handler)))
  (cancel-command [obj]
    (.close (:socket-connection obj))
    (merge obj (update-state! obj :cancelled)))
  (get-status [obj]
    ;(println "scp get-status")
    (let [data @(:scp-object obj)
          bytes-transferred @(:bytes-sent-atom data)
          current-time (System/currentTimeMillis)
          rate (/ bytes-transferred
                  (- current-time (:start-time obj)))

          time-remaining (long (/ (- (:file-length obj) bytes-transferred)
                                  rate))

          percent-complete (* 100 (/ bytes-transferred (:file-length obj)))]
      ;(println "Time Remaining" time-remaining)
      {:completion-time  (+ current-time time-remaining)
       :percent-complete percent-complete
       ;:time-remaining time-remaining
       })))

(defn make-scp-upload [finish-handler command-map]
  (->scp-upload :state finish-handler command-map))

(defn print-scp-state [cmd]
  (println "scp bytes sent" @(:bytes-sent-atom @(:scp-object cmd)) "\n")
  #_(let [scp-object (:scp-object cmd)]
      (doseq [[k v] @scp-object]
        (println "Got " k @v))))

;; Functions to test from repl
; Start SCP , sleep, check scp state, wait until scp finished
(defn start-and-check [args]
  (println "Got args" args)
  (let [cmd (make-scp-upload (fn [id exitValue other-m]
                               (println "SCP Command finished" id "with exit-value" exitValue "other-info" other-m))
                             {:id      "scp-123"
                              :argsmap {:fromfile (first args)
                                        :host     (second args)
                                        :tofile   (nth args 2)
                                        }})
        cmd (start-command cmd)
        result-future (:scp-result-future cmd)
        ]
    (println "SCP Started started")

    (println "Sleep 2 sec")
    (Thread/sleep 2000)
    (print-scp-state cmd)
    (println "Waiting for scp finish or timeout 2000")
    (deref result-future 2000 :timeout)
    (print-scp-state cmd)
    @result-future
    (println "scp returned" @result-future)
    (print-scp-state cmd)
    cmd))

; start scp, sleep, check scp state, cancel scp state.
(defn start-and-cancel [args]
  (println "Got args" args)
  (let [cmd (make-scp-upload (fn [id exitValue other-m]
                               (println "SCP Command finished" id "with exit-value" exitValue "other-info" other-m))
                             {:id      "scp-123"
                              :argsmap {:fromfile (first args)
                                        :host     (second args)
                                        :tofile   (nth args 2)
                                        }})
        cmd (start-command cmd)
        result-future (:scp-result-future cmd)
        ]
    (println "SCP Started started")
    (print-scp-state cmd)
    (println "Waiting for scp finish or timeout 2000")
    (deref result-future 2000 :timeout)
    (print-scp-state cmd)
    (println "Cancel SCP")
    (cancel-command cmd)
    (println "Waiting for cancel to finish or timeout" (deref result-future 2000 :timeout))
    (when-not (realized? result-future)
      (println "Cancel operation timed out. FIX ME."))
    (println "scp returned" @result-future)
    (print-scp-state cmd)
    cmd))

(defn send-file [src-file host dest-file]
    "Assumes non interactive mode.
    i.e public key authentication and destination host key is already in known_hosts.
    Return an object that can be derefd to receive number of bytes transferred"
  (let [_ (println "Ssh dir:" (OpenSSHKnownHosts/detectSSHDir))
        ssh (SSHClient.)
          _ (.loadKnownHosts ssh)
          _ (.connect ssh host)
        _ (println "Auth using name:" (-> (System/getProperties) (.get "user.name")))
          _ (.authPublickey ssh (-> (System/getProperties) (.get "user.name")))

          fileo (make-counting-fsf src-file)
          transfer (.newSCPFileTransfer ssh)
          _ (.upload transfer fileo dest-file)]
      (println "bytes sent" @fileo)
      (println "file size:" (.getLength fileo))
      (println "Host:" host ", src-file:" src-file ", destination-file:" dest-file)
      fileo))

(defn test-send-file []
  "My quick test"
  ; Note: When multiple users (ex: prakash or jenkins) try to upload the file to the same location,
  ; this test will succeed for one of the users and fail for the rest.
  ; So we change the destination file prefixed with user-name
  (try
    (send-file #_(str (System/getProperty "user.home") "/.ssh/known_hosts")
      "test/plant/small-file"
      "192.168.11.100"
      (str "/tmp/" (-> (System/getProperties) (.get "user.name")) "-small-file") )
    (catch Exception e
      (println "Test send file caught exception: " (.getMessage e)))))