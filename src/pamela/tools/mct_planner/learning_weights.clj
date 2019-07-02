;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.learning-weights
  (:require [pamela.tools.mct-planner.util :as ru]))

; Another variant of learning that uses counts of success and failures instead of
; normal distribution

(defrecord monte-count-learner [values storage]
  ; values is a vector of integer values and storage is atom of {}
  )

(defn get-enum-dist [learner]
  (last (get-in @(:storage learner) [:pdf-a-mass])))

(def initial-weights 1000)
(def lambda-succ 10)
(def lambda-fail -5)

(defn make-monte-learner [vals-vec]
  (let [initial-prob initial-weights
        pdf-a (reduce (fn [res val]
                        (conj res {val initial-prob})) (sorted-map) vals-vec)
        obj (->monte-count-learner vals-vec (atom {:pdf-a [pdf-a]
                                             :pdf-a-mass [(ru/make-enum-distribution pdf-a)]
                                             }))
        ]
    obj))

(defn get-pdf-a [learner]
  (last (get-in @(:storage learner) [:pdf-a]) ))

(defn update-pdf-a [learner a]
  (try
    (swap! (:storage learner) update-in [:pdf-a] conj a)
    (swap! (:storage learner) update-in [:pdf-a-mass] conj (ru/make-enum-distribution a))
    (catch Exception e (do
                         (println "Error updating a")
                         (println a)
                         )))

  )

(defn adjust-pdf [a value lambda]
  ;(println "adjust" lambda value a)
  (let [new-val (+ (get a value) lambda)
        new-val (if (> new-val 0)
                  new-val 0)
        ]
    (assoc a value new-val)))

(defn get-pdf-a-as-vec [learner idx]
  (let [pdf (if idx
              (nth (get-in @(:storage learner)  [:pdf-a]) idx)
              (last (get-in @(:storage learner)  [:pdf-a])))]
    (reduce (fn [res val]
              (conj res (get pdf val))) [] (:values learner)))
  )

(defn get-pdf-a-mass-as-vec [learner idx]
                       (let [enum-dist (if idx
                                         (nth (get-in @(:storage learner)  [:pdf-a-mass]) idx)
                                         (last (get-in @(:storage learner)  [:pdf-a-mass])))
                             pdf-mass (reduce (fn [res x]
                                                (conj res {(.getKey x) (.getValue x)}))
                                              {} (.getPmf enum-dist))]
                         (reduce (fn [res val]
                                   (conj res (get pdf-mass val))) [] (:values learner))))

(defn sample-from-monte [learner]
  (.sample (get-enum-dist learner)))

(defn update-monte [learner value succ]
  ;(println "update for value" value succ)
  (let [lambda (if succ
                 lambda-succ
                 lambda-fail)

        a (get-pdf-a learner)
        updated (adjust-pdf a value lambda)
        ;updated (shift-pdf-to-zero updated)
        ]
    ;(println "updated" updated)
    (update-pdf-a learner updated)
    updated))