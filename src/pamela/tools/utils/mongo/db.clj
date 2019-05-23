;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

; Collection of Mongo DB functions

(ns pamela.tools.utils.mongo.db
  (:require [pamela.tools.utils.tpn-json :as tpn-json]
            [pamela.tools.utils.mongo.state :refer :all]

            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.query :as mq]

            [clojure.data.json :as cl-json]
            [clojure.java.io :refer :all]
            [clojure.pprint :refer :all]))

(def mongo-coll "rmq-log")

(defn get-data
  "Read from mongo ordered by increasing a-key"
  [data-set db a-key]
  (println "Getting data")
  (let [mqdata (mq/with-collection db data-set
                                   (mq/find {})
                                   (mq/sort (array-map a-key 1))
                                   #_(mq/limit 10)
                                   )]
    ;(println "type" (type mqdata))
    ;(println "count" (count mqdata))
    (into [] mqdata)))

(defn connect!
  "Connection wrapper with caching connection"
  [& {:keys [host port]}]
  (let [opts {}
        opts (when host (merge opts {:host host}))
        opts (when port (merge opts {:port port}))]
    (if-not (get-connection)
      (do (update-state! :connection (mg/connect opts))
          (get-connection))
      (binding [*out* *err*]
        (println "Already connected !!")))))

(defn connected? []
  (if (get-connection) true false))

(defn get-db
  "Caching wrapper for mg/get-db"
  ([]
   (:db @state))

  ([name]
   (if-not (get-connection)
     (binding [*out* *err*]
       (println "DB Connection is nil"))
     (let [db-key (keyword name)]
       (if (db-key @state)
         (db-key @state)
         (do
           (update-state! db-key (mg/get-db (get-connection) name))
           (update-state! :db (db-key @state))
           (db-key @state)))))))


(defn to-db
  "Inserts given document to default db"
  [m]
  #_(when-not (get-db)
    (binding [*out* *err*]
      (println "No Mondo DB object")))
  (when (get-db)
    (mc/insert (get-db) mongo-coll (merge m {:ts (System/currentTimeMillis)}))))

(defn shutdown
  "Closes connection and resets state"
  []
  (let [db (get-db)]
    (when db
      (mg/disconnect (get-connection))
      (remove-from-state (keyword (.getName db)))
      (remove-from-state :db)
      (remove-from-state :connection))))

(defn file-to-mongo
  "Insert json lines from a file to mongodb"
  [file-name db-name collection]

  (let [_ (connect!)
        db (get-db db-name)]

    (with-open [rdr (reader file-name)]
      (doseq [line (line-seq rdr)]

        (let [as-map (try (tpn-json/map-from-json-str line)
                          (catch Exception _
                            (println "Error parsing:" line)
                            nil))]
          (when as-map
            #_(pprint as-map)
            (mc/insert db collection as-map)
            #_(println "inserted")))))))

(defn insert-batch
  "Inserts given list of docs into the DB.
  'db' is MDB object, collection is string and docs is a list"
  [db collection docs]
  ;(println "insert-batch" db collection docs)
  (mc/insert-batch db collection docs))

(defn insert [db-name collection doc]
  (mc/insert (get-db db-name) collection doc))