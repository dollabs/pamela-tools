;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant-examples.hmm
  "Functions to work with Hidden Markov Models"
  (:require [pamela.tools.plant.util :as util]
            [clojure.set :as set]
            [clojure.pprint :refer :all])
  (:import (be.ac.ulg.montefiore.run.jahmm OpdfInteger Hmm ObservationInteger)))

(defn convert-pdf-to-range [m]
  "Given pdf function for discrete variables as m {:k1 0.1 :k2 0.2 :k3 0.3 :k4 0.4}
  return [[:k1 0.1] [:k2 0.3] [:k3 0.6] [:k4 1.0]]"
  (into [] (sort-by (fn [kv]
                      (second kv))
                    < (into [] (dissoc (reduce (fn [result [k v]]
                                                 (let [sum (+ v (:sum result))]
                                                   (merge result {k sum :sum sum})))
                                               {:sum 0} m) :sum)))))

(defn convert-pdf-to-range-matrix
  "Assume input is matrix represented as map"
  [matrix]
  (reduce (fn [r [k v]]
            (merge r {k (convert-pdf-to-range v)}))
          {} matrix))

(defn choose-with-probability
  "Assume map-or-vec is probability distribution function represented as {:k1 0.1 :k2 0.9}
  or vector [[:k1 0.1] [:k2 1.0]] returned by (convert-pdf-to-range {:k1 0.1 :k2 0.9}).
  return :k1 or :k2 according to their probabilities assuming uniform distribution
  rng is an optional java.util.Random instance"
  [map-or-vec & [rng]]
  (let [range (if (map? map-or-vec)
                (convert-pdf-to-range map-or-vec)
                map-or-vec)
        p     (if-not rng (rand)
                          (.nextDouble rng))]
    ;(println "Choose with prob" range p)
    (first (reduce (fn [[kr kv] [k v]]
                     ;(println kr kv k v)
                     (if (< p kv)
                       [kr kv]
                       [k v]))
                   (first range)
                   range))))

(defprotocol hmmI

  (indices [model]
    "return indices for transition and emission symbols
    {:tr-index {:t1 1 :t2 2}
     :em-index {:e1 1 :e2 2}}")
  (init! [model]
    "1. Initialize initial state
     2. Assign indices to transition and emission symbols
     3. Convert transition and emission probabilities to range
        :tr-p-range :em-p-range")
  (to-emission-indices [model symbols]
    "Convert emission symbols to indices using :em-index")
  (from-emission-indices [model inds]
    "Convert emission indices to symbols")
  (to-transition-indices [model symbols]
    "Convert transition symbols to indices using :tr-index")
  (from-transition-indices [model inds]
    "Convert transition/state indices to symbols"))

(defrecord model [pi transitions emissions initial-state]
  hmmI
  (indices [model]
    (let [tr-index (util/keys-to-indices (keys (:transitions model)))
          em-index (util/keys-to-indices (util/emission-set-keys (:emissions model)))]
      {:tr-index         tr-index
       :tr-index-reverse (set/map-invert tr-index)
       :em-index         em-index
       :em-index-reverse (set/map-invert em-index)
       }))
  (init! [model]
    (let [indices    (indices model)
          state      (choose-with-probability (:pi model))
          tr-p-range {:tr-p-range (convert-pdf-to-range-matrix (:transitions model))}
          em-p-range {:em-p-range (convert-pdf-to-range-matrix (:emissions model))}
          model      (merge model indices {:initial-state state} tr-p-range em-p-range)]
      model))
  (to-emission-indices [model symbols]
    (replace (:em-index model) symbols))
  (to-transition-indices [model symbols]
    (replace (:tr-index model) symbols))
  (from-emission-indices [model inds]
    (replace (:em-index-reverse model) inds))
  (from-transition-indices [model inds]
    (replace (:tr-index-reverse model) inds))
  )

(defn make-java-hmm
  "Given record object, add jahmm instance and other necessary attributes to it"
  [model]

  ;(println "Emissions")
  ;(pprint (:emissions model))
  (let [indexed-pi          (util/map-to-vector (set/rename-keys (:pi model) (:tr-index model)))
        indexed-transitions (util/map-to-matrix (:transitions model) (:tr-index model) (:tr-index model))
        indexed-emissions   (util/map-to-matrix (:emissions model) (:tr-index model) (:em-index model))
        opdf                (reduce (fn [result row]
                                      ;(println "row" result row)
                                      (conj result (OpdfInteger. (into-array Double/TYPE row))))
                                    [] indexed-emissions)
        ]
    #_(pprint {:indexed-pi          indexed-pi
               :indexed-transitions indexed-transitions
               :indexed-emissions   indexed-emissions
               :j-pi                (into-array Double/TYPE indexed-pi)
               :j-transitions       (into-array (map double-array indexed-transitions))
               :j-emissions         (into-array (map double-array indexed-emissions))
               :opdf                opdf
               })

    {:indexed-pi          indexed-pi
     :indexed-transitions indexed-transitions
     :indexed-emissions   indexed-emissions
     :j-hmm               (Hmm. (into-array Double/TYPE indexed-pi)
                                (into-array (map double-array indexed-transitions))
                                opdf)}))

(defn to-transition-matrix [jhmm]
  (reduce (fn [r-result row-index]
            ;(println r-result row-index)
            (conj r-result (reduce (fn [c-result col-index]
                                     (conj c-result (.getAij jhmm row-index col-index)))
                                   [] (range (.nbStates jhmm)))))
          [] (range (.nbStates jhmm))))

(defn OpdfInteger-to-vector [opdf]
  (reduce (fn [result entry]
            (conj result (.probability opdf (ObservationInteger. entry)))
            )
          [] (range (.nbEntries opdf))))

(defn to-emission-matrix [jhmm]
  (reduce (fn [r-result row-index]
            (conj r-result (OpdfInteger-to-vector (.getOpdf jhmm row-index))))
          [] (range (.nbStates jhmm))))

(defn to-pi-vector [jhmm]
  (reduce (fn [result idx]
            (conj result (.getPi jhmm idx)))
          [] (range (.nbStates jhmm))))

(defn j-hmm-to-matrix [hmm]
  ;(println "Transition matrix" (to-transition-matrix hmm))
  ;(println "Emission matrix" (to-emission-matrix hmm))
  {:tr-matrix (to-transition-matrix hmm)
   :em-matrix (to-emission-matrix hmm)
   :pi-vector (to-pi-vector hmm)})

(defn jahmm-to-model
  "Given Hmm Object and model record, return learned parameters"
  [j-hmm model]
  (println "J-HMM" j-hmm)
  (let [as-matrix (j-hmm-to-matrix j-hmm)]
    ;(println "As Matrix")
    ;(pprint as-matrix)
    ;(println (util/matrix-to-map (:tr-matrix as-matrix) (:tr-index-reverse model) (:tr-index-reverse model)))
    ;(println (util/matrix-to-map (:em-matrix as-matrix) (:tr-index-reverse model) (:em-index-reverse model)))
    ;(println (util/vector-to-map (:pi-vector as-matrix) (:tr-index-reverse model)))
    {:pi           (util/vector-to-map (:pi-vector as-matrix) (:tr-index-reverse model))
     :transitions  (util/matrix-to-map (:tr-matrix as-matrix) (:tr-index-reverse model) (:tr-index-reverse model))
     :emissions    (util/matrix-to-map (:em-matrix as-matrix) (:tr-index-reverse model) (:em-index-reverse model))
     :var-bindings (:var-bindings model)}))

(defn make-model [m]
  (let [model (map->model m)
        model (init! model)
        model (merge model (make-java-hmm model))
        ]
    model))

