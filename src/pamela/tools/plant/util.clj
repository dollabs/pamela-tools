;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant.util
  (:require [clojure.set :as set]
            [clj-time.local :as l]
            [clj-time.format :as f]
            [clj-time.core :as c]))


; A unique id to generate sequence of ids for each method invocation

(def custom-formatter (f/formatter "MMdd-HH.mm.ss.S-" (c/default-time-zone)))

(defn make-time-as-str []
  (f/unparse custom-formatter (l/local-now)))

(defonce counter-prefix (str "dispatcher-" (make-time-as-str)))
;(defonce counter-prefix "dispatcher-")
(defonce method-counter (atom 1))

(defn make-method-id [& [prefix]]
  (let [id (keyword (str (or prefix counter-prefix) "-" @method-counter))]
    (swap! method-counter inc)
    id))

(defn make-method-ids [count]
  (let [begin @method-counter
        end (+ begin count)
        ids (range begin end)
        ids (map (fn [id]
                   (keyword (str counter-prefix "-" id))) ids)]

    (swap! method-counter (fn [_] end))
    ids))

(defn keys-to-indices [v]
  "To assign indices to keywords. Returns a map with keyword as key and integer as value"
  (into {} (map-indexed (fn [index item]
                          {item index}) v)))

(defn emission-set-keys
  "Given emissions map, return set of emission keywords"
  [m]
  (reduce (fn [result [_ val]]
            (into result (keys val))) #{} m))

(defn map-to-vector [m]
  "Assume keys of m are indices to the vector.
  Assign the values in vector according to keys and return the vector"
  (let [len (apply max (keys m))]
    (reduce (fn [v [index value]]
              (assoc v index value))
            (vec (repeat len nil)) m)))

(defn map-to-matrix [m r2i c2i]
  "Converts a map of maps to matrix
   r2i is a map of row keys to index lookup
   c2i is a map of column keys to index lookup"
  ;(println "Matrix" m)
  ;(println "Row keys" r2i)
  ;(println "Column keys" c2i)
  (let [rows (map-to-vector (set/rename-keys m r2i))]

    ;(println "Rows " rows)
    (reduce (fn [v val]
              ;(println v val)
              (conj v (map-to-vector (set/rename-keys val c2i))))
            [] rows)))

(defn vector-to-map
  "Given vector, using the index to lookup a key in idx-to-key
   return map of key and val (at index)"
  [vec idx-to-key]
  (reduce (fn [result index]
            (conj result {(get idx-to-key index) (get vec index)}))
          {} (range (count vec))))

(defn matrix-to-map
  "Given a matrix, convert row indices to keys of a map according to i2r,
   column indices to keys according to i2c"
  [mat i2r i2c]
  ;(println "matrix-to-map" mat i2r i2c)
  (reduce (fn [r-result row-index]
            (conj r-result {(get i2r row-index)
                            (vector-to-map (get mat row-index) i2c)}))
          {} (range (count mat))))


