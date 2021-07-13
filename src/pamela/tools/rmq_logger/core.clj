;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.rmq-logger.core
  "Application to listen for all RMQ messages and log to stdout"
  (:gen-class)
  (:require [pamela.tools.utils.util :as util]
            [pamela.tools.utils.rabbitmq :as rmq]

            [langohr.core :as lcore]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.channel :as lch]

            [clojure.tools.cli :as cli]
            [clojure.data.json :as cl-json]
            [clojure.string :as string]
            [clojure.pprint :refer :all])

  (:import (java.text SimpleDateFormat)
           (java.util Date)
           (java.util.concurrent LinkedBlockingQueue TimeUnit)
           (java.net ConnectException)))

(defonce sdf (SimpleDateFormat. "yyyy-MMdd-HHmm"))
(def received-count 0)

(def repl true)
(defonce last-ctag nil)
(defonce mongo? false)                                      ;default value.
(defonce msg-q (new LinkedBlockingQueue))
(defonce read-thread nil)
(def sig-terminate false)
(def clear-queue false)
(def truncated-output-length "If non-nil, this is the length of the raw-data output"
  (atom nil))

(defn update-mongo?-p! "Update state of mongo? var"
  [new-val]
  (def mongo? new-val))

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default "tpn-updates"]
                  [nil "--dbhost dbhost" "Mongo DB Host"]
                  [nil "--dbport dbport" "Mongo DB Port" :parse-fn #(Integer/parseInt %)]
                  ["-n" "--name dbname" "Mongo DB Name" :default (str "rmq-log-" (.format sdf (Date.)))]
                  ["-l" "--length length" "Truncated length of the raw-data output"
                   :default nil :parse-fn #(Integer/parseInt %)]
                  [nil "--clear-queue true/false" "Upon Control-C, this option will clear Q, wait for 2 seconds then exit."
                   :default false :parse-fn #(Boolean/parseBoolean %)]
                  ["-?" "--help"]])

(defn handle-message
  "Non threaded writer."
  [payload exchange routing-key content-type time]

  (def received-count (inc received-count))
  (when (zero? (mod received-count 10000))
    (util/to-std-err
      (println "Messages received so far" received-count)))

  (let [bin-type? (if (= content-type "application/x-binary")
                    true false)
        st        (if-not bin-type? (String. payload "UTF-8"))
        m         (if-not bin-type? (util/map-from-json-str st)
                                    {:image "binary-data not displayed"})
        m         (conj m {:received-routing-key routing-key :exchange exchange})]
    ;(clojure.pprint/pprint metadata)
    ;(println "--- from exchange:" exchange ",routing-key:" routing-key)
    (when repl (clojure.pprint/pprint m))
    (let [untruncated-string (cl-json/write-str m)]
      (println "raw-data," time "," (if @truncated-output-length
                                      (subs untruncated-string
                                            0 (min @truncated-output-length
                                                   (count untruncated-string)))
                                      untruncated-string)))))

(defn threaded-writer "Post the message to the Q"
  [payload exchange routing-key content-type time]
  (let [state (.offer msg-q {:payload payload :exchange exchange :routing-key routing-key :content-type content-type
                             :time    time})]
    (if-not state (util/to-std-err (println "Error: msg-q is full")))))

(defn threaded-read []
  (util/to-std-err
    (println "Blocking read on msg-q on thread" (.getName (Thread/currentThread))))

  (try
    (while (false? sig-terminate)                           ;until we receive sigterm
      (let [m (.poll msg-q 1 TimeUnit/SECONDS)]
        (when m (handle-message (:payload m) (:exchange m) (:routing-key m) (:content-type m) (:time m)))))
    (catch InterruptedException _
      (util/to-std-err (println "threaded-read interrupted"))))

  (when sig-terminate
    (util/to-std-err (println "Clearing Q due to SIGTERM. Pending messages" (.size msg-q)))
    (let [before (System/currentTimeMillis)
          _      (loop [start-time (System/currentTimeMillis)
                        now        (System/currentTimeMillis)]
                   (if (>= now (+ 2000 start-time))
                     nil
                     (let [m              (.poll msg-q 1 TimeUnit/SECONDS)
                           new-start-time (if (and clear-queue m)
                                            ; will exit 2 seconds after last message has been handled
                                            (+ 2000 now)
                                            ; Guaranteed exit in 2 seconds but may leave unprocessed messages in the Q.
                                            start-time)]

                       #_(util/to-std-err (println "Waiting time in millis before definite exit.. " (- (+ 2000 start-time) now)))
                       (when m (handle-message (:payload m) (:exchange m) (:routing-key m) (:content-type m) (:time m)))
                       (recur new-start-time (System/currentTimeMillis)))))
          after  (System/currentTimeMillis)]
      (util/to-std-err (println "Sig Terminate time:" (- after before)))
      (util/to-std-err
        (println "Messages received so far" received-count)
        (println "Done Clearing Q due to SIGTERM. Pending messages" (.size msg-q)))))


  #_(util/to-std-err (println "done -- threaded-read")))

(defn incoming-msgs [_ metadata ^bytes payload]
  ;(pprint metadata)
  (threaded-writer payload (:exchange metadata) (:routing-key metadata) (:content-type metadata) (System/currentTimeMillis)))

(defn setup-threaded-read []
  (when read-thread
    (util/to-std-err (println "previous read-thread is active. Will interrupt"))
    (.interrupt read-thread))
  (let [jth (Thread. threaded-read)]
    (.setName jth "msg-read-thread")
    (.setDaemon jth true)
    (.start jth)
    (def read-thread jth)))

(defn usage [options-summary]
  (->> ["Program to listen for all messages on a RMQ exchange"
        ""
        "Usage: java -jar rmq_logger-XXX-standalone.jar [options]"
        ""
        "Options:"
        options-summary
        ""]
       (string/join \newline)))

(defn exit [code]
  (when-not repl
    (System/exit code))
  (if repl
    (println "in repl mode. Not exiting")))

(defn shutdown []
  (util/to-std-err (println "Received Control-C "))
  (def sig-terminate true)
  (.join read-thread))

; Print messages as they are flowing through the channel
(defn -main
  "Tail messages through exchange"
  [& args]
  (def foo-args args)
  ;; (println "args:" args)
  ;; (println "args type:" (type args))
  (let [parsed            (cli/parse-opts args cli-options)
        ;_ (println parsed)
        ch-name           (get-in parsed [:options :exchange])
        host              (get-in parsed [:options :host])
        port              (get-in parsed [:options :port])
        help              (get-in parsed [:options :help])
        clear-q (get-in parsed [:options :clear-queue])
        _                 (def repl false)
        _                 (when help
                            (println (usage (:summary parsed)))
                            (exit 0))
        connection        (try (lcore/connect {:host host :port port})
                               (catch ConnectException e
                                 (util/to-std-err (println (.getMessage e)
                                                           (str "for " host ":" port)))))
        _                 (when-not connection (exit 1))
        channel           (lch/open connection)
        _                 (le/declare channel ch-name "topic")
        queue             (lq/declare channel)
        qname             (.getQueue queue)
        _                 (lq/bind channel qname ch-name {:routing-key "#"})
        ctag              (lc/subscribe channel qname incoming-msgs {:auto-ack true})
        ;dbhost (get-in parsed [:options :dbhost])
        ;dbport (get-in parsed [:options :dbport])
        ;_ (when dbhost
        ;    (mongo.db/connect! :host dbhost :port dbport)
        ;    (mongo.db/get-db (get-in parsed [:options :name]))
        ;    (update-mongo?-p! true))
        truncation-length (get-in parsed [:options :length])]
    (.addShutdownHook (Runtime/getRuntime) (Thread. shutdown))
    (def clear-queue clear-q)
    (util/to-std-err (println "Will clear queue upon Control-C" clear-queue))
    (reset! truncated-output-length truncation-length)
    (if @truncated-output-length
      (println "truncated-output-length:" @truncated-output-length))
    (util/to-std-err (println "Tail Config" (:options parsed)))
    (setup-threaded-read)
    (when last-ctag
      (rmq/cancel-subscription (first last-ctag) (second last-ctag)))
    ; conj for list pushes to the front, so we push channel then ctag.
    ; So, we get ctag = (first last-ctag), and channel = (second last-ctag)
    (def last-ctag (conj last-ctag channel ctag))
    ctag))
