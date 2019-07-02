;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.entropy
  (:require [pamela.tools.mct-planner.util :refer :all]
            [clojure.pprint :refer :all]))

(defn entropy [x]
  (cond (= 0 x)
        0
        :else
        (- (* x (log2 x)))))

#_(defn random-bound [bounds]
  "Assumes bounds is [lb ub]
  Returns a random value between lb and ub
  If lb is infinity, return random value between lb and MAX_VALUE"
  (let [duration (duration bounds)
        duration (if (= :infinity duration)
                   Integer/MAX_VALUE
                   duration)
        ]
    (if (number? duration)
      (+ (rand-int duration) (first bounds))
      (first bounds))))

(defmulti value-entropy (fn [expr]
                          (:symbol expr)))

(defmethod value-entropy :default [expr]
  (binding [*out* *err*]
    (println "Impl value-entropy for type" (:symbol expr) "of" expr)
    0))

(defmethod value-entropy 'in-range [expr]
  (entropy (probability-distribution (:value expr))))

(defn expression-entropy [expr bound]
  "Return a [to-var entropy] for the given expression"
  (let [ent (map (fn [var]
                   (if (get bound var) 1 0))
                 (:from-vars expr))
        value-ent (if (and (:controllable expr) (:value expr))
                    (value-entropy expr)
                    0)]
    [(:to-var expr) (reduce + (conj ent value-ent))]))

(defn sumed-entropy [entropies]
  "handles the case when more than 1 expression contributes to the entropy of var"
  (loop [et-local entropies
         result {}]
    (if (empty? et-local)
      result
      (let [et (first et-local)
            key (first et)
            val (if (get result key)
                  (+ (result key) (second et))
                  (second et))]
        (recur (rest et-local) (conj result {key val}))))))

(defn compute-entropy [exprs bindings]
  "Returns a list of [var val] ordered by decreasing entropy values.
  The list contains only unbounded vars."
  (let [bound (get-bound-vars bindings)
        entropies (map (fn [expr]
                         (expression-entropy expr bound)) exprs)
        sumed (sumed-entropy entropies)
        sorted (sort-by second > sumed)
        sorted-unbound (remove (fn [entropy]
                                 (get bound (first entropy))) sorted)]
    ;(pprint entropies)
    ;(pprint sumed)
    ;(pprint sorted)
    (println "Entropy sorted for unbound vars")
    (pprint sorted-unbound)
    sorted-unbound
    ))