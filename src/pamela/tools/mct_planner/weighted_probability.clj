;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.weighted-probability
  (:require [pamela.tools.mct-planner.util]
            [clojure.pprint :refer :all])
  (:import [org.apache.commons.math3.distribution EnumeratedDistribution]
           [org.apache.commons.math3.util Pair]))


(defrecord weighted-distribution [weights])

(defn make-printable [weights]
  {:weights  (:weights weights)                             ;removes reference to object type
   :prob-obj (reduce (fn [res x]

                       (conj res [(.getKey x) (.getValue x)]))
                     [] (.getPmf (:prob-obj weights)))})

(defmethod print-method weighted-distribution [wd ^java.io.Writer w]
  (.write w (pr-str (make-printable wd))))

(defn pprint-weight-distribution [weights]
  (pprint (make-printable weights)))

(. clojure.pprint/simple-dispatch addMethod weighted-distribution pprint-weight-distribution)

(defn make-distribution
  "Weight-map is map of keys and their weights"
  [weight-map]
  (let [dist (new EnumeratedDistribution (map (fn [[key val]]
                                                (new Pair key (double val)))
                                              weight-map))
        wdist (merge (->weighted-distribution weight-map) {:prob-obj dist})]
    wdist))

(defn sample-weighted-distribution [wd]
  (.sample (:prob-obj wd)))

; https://stackoverflow.com/questions/14464011/idiomatic-clojure-for-picking-between-random-weighted-choices
; A trivial approach

(defn weighted-sample
  "weights is {:na1 5 :key integer-number} where integer-number should be postive as 0 will never be selected
  TODO: Handle case for infinity
  Randomly returns a sample according to its weight"
  [weights]
  (rand-nth (into [] (flatten (map (fn [[item weight]] (repeat weight item)) weights)))))

(defn static-uncertainty-sample
  "We choose infinity with 1% probability" [weights])

; Wrapper around http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/index.html
; to support learning

(def bounds [[0 10] [0 11] [0 12]])
(def code-len (map (fn [x]
                     (pamela.tools.mct-planner.util/bounds-code-length x))
                   bounds))

(def pair (new Pair :na1 10))
(def x (new EnumeratedDistribution [(new Pair :na1 3.4594316186372978) (new Pair :na1 3.5849625007211565)]))
(def x (new EnumeratedDistribution [(new Pair :na1 10.0) (new Pair :na1 100.0)]))

; https://github.com/jbranchaud/til/blob/master/clojure/swap-two-items-in-a-vector.md
(defn swap-items
  [items i j]
  (println "swapping" i j)
  (assoc items i (items j) j (items i)))

; higher uncertainty should have less weight and hence less probability.
; But EnumerationDistribution gives higher probability to higher values.
(defn swap-uncertainty
  "Swap uncertainty values so that higher values are less likely to be selected"
  [unc]
  (let [sorted (sort-by second unc)
        keys (map first sorted)
        vals (into [] (map second sorted))
        len (count sorted)
        mid (int (/ len 2))
        vals-swapped (reduce (fn [res idx]
                               (swap-items res idx (- len idx 1))) vals (range mid))]

    (println "sorted" sorted)
    (println "keys" keys)
    (println "vals" vals)
    (println "len mid" len mid)
    (println "vals swapped" vals-swapped)
    (zipmap keys vals-swapped)))

(def uncertainty (make-distribution {:na1 10 :na2 20 :na3 30}))

; Need positive values
;(def uncertainty (make-distribution {:na1 -10 :na2 -20 :na3 -30}))


