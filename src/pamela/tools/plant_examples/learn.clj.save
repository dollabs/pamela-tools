;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant-examples.learn
  "Learning for biased coin example"
  (:require [pamela.tools.utils.mongo.db :as mdb]
            [pamela.tools.plant-examples.hmm :as hmm]
            [pamela.tools.plant.util :as util]

            [monger.collection :as mc]
            [monger.query :as mq]

            [clojure.pprint :refer :all])

  (:import (be.ac.ulg.montefiore.run.jahmm.learn BaumWelchLearner)
           (be.ac.ulg.montefiore.run.jahmm ViterbiCalculator)
           (be.ac.ulg.montefiore.run.jahmm ObservationInteger)))

; HMM Learning code

(def mdb-name "coin-plant")
(def collection-name "observations")
(def learned-hmm-collection "hmm")
(def observation-key :observed-face)


;(defonce mdb-connection (mdb/connect!)); FIXME TODO. Uncommented because at compile time it expects a MDB instance.
(def bw (BaumWelchLearner.))
(def bwiterations 100)

; Initial guess off my 10% from plant model for transitions
(def pamela-model {;probability of initial state
                   :pi           {:biased 0.45 :unbiased 0.55}

                   ;state transition probabilities
                   :transitions  {
                                  :biased   {:biased 0.90 :unbiased 0.10}
                                  :unbiased {:unbiased 0.90 :biased 0.10}}
                   ;emission probabilities
                   :emissions    {
                                  :biased   {:heads 0.72 :tails 0.28}
                                  :unbiased {:heads 0.55 :tails 0.45}}
                   :var-bindings {:biased   {:transtions {:unbiased "tBU" :biased "tBB"}
                                             :emissions  {:tail "eBT" :head "eBH"}}
                                  :unbiased {:transitions {:unbiased "tUU" :biased "tUB"}
                                             :emissions   {:tail "eUT" :head "eUH"}}}})

(def pamela-hmm (hmm/make-model pamela-model))

(defn to-integer-observations [a-list]
  "Returns java arraylist of observation objects"
  (java.util.ArrayList. (map (fn [x]
                               (ObservationInteger. x))
                             a-list)))

(defn integer-observations-from-2d-array [matrx]
  "Each row is a list of integers"
  (java.util.ArrayList. (map (fn [a-list]
                               (to-integer-observations a-list)) matrx)))

(defn get-observations [db-name collection observation-key]
  (let [db (mdb/get-db db-name)]
    ; Convert observation values to keywords
    (map (fn [obs]
           (keyword (observation-key obs)))
         (mc/find-maps db collection {} {observation-key true}))))

(defn get-observations-samples [db-name collections]
  (mc/find-maps (mdb/get-db db-name) collections))

(defn split-observations [observations]
  (let [lnth (count observations)
        both (split-at (/ lnth 2) observations)]
    ;(println "All" lnth observations)
    ;(println "Both" (count both))
    {:sample-data (first both)
     :test-data   (second both)}))

(defn extract-observations [samples obs-key]
  (map (fn [obs]
         (keyword (obs-key obs))) samples))

(defn learn-bw
  "data is seq of symbols, model is record"
  [data model iterations]
  (when (:j-hmm model)
    (let [bw (BaumWelchLearner.)
          data-ints (hmm/to-emission-indices model data)]
      (.setNbIterations bw iterations)
      (println "BW Iterations" (.getNbIterations bw))
      (.learn bw (:j-hmm model) (integer-observations-from-2d-array (list data-ints))))))

(defn model-to-db [model db-name]
  (let [partial (select-keys model #{:pi :transitions :emissions :var-bindings})
        partial (merge partial {:ts (System/currentTimeMillis)})]
    (println "model going to db" partial)
    (mdb/insert db-name learned-hmm-collection partial)))

(defn get-hmms-from-db [db-name]
  (mq/with-collection (mdb/get-db db-name) learned-hmm-collection
                      (mq/find {})
                      (mq/sort {:ts -1})))

(defn learn-bw-db
  "1. Initialize HMM from DB
        If not available, initialize HMM from model
        If DB has HMM, use the most recent one
   2. Add the learned model to DB"
  [data initial-model iterations db-name]
  (let [
        ;l-hmms  (mc/find-maps (mdb/get-db db-name) learned-hmm-collection)

        l-hmms (get-hmms-from-db db-name)
        model (if-not (empty? l-hmms)
                (first l-hmms)
                initial-model)
        model-r (hmm/make-model model)                      ;converted to our internal record
        j-learned (learn-bw data model-r iterations)
        learned-model (hmm/jahmm-to-model j-learned model-r)]

    (println "From DB")
    (pprint l-hmms)
    (println "Model record")
    (pprint model-r)
    (println "learned model")
    (pprint learned-model)
    (if (empty? l-hmms)
      (model-to-db initial-model db-name))
    ; Now put the learned model to db
    (model-to-db learned-model db-name)))

(defn test-bw-db []
  (let [data-objs (get-observations-samples mdb-name collection-name)
        both (split-observations data-objs)
        data-1st-half (:sample-data both)
        data-sym (extract-observations data-1st-half :observed-face)]

    (learn-bw-db data-sym pamela-model bwiterations mdb-name)))

(defn apply-viterbi [data-as-symbols j-learned model]
  (let [data-as-ints (hmm/to-emission-indices model (into [] data-as-symbols))
        vit (ViterbiCalculator. (to-integer-observations data-as-ints)
                                j-learned)
        states-in (map int (.stateSequence vit))]
    states-in))

(defn update-pi
  "if prev-state is not nil, then it is an integer that represents state
  pi of this state is 1 and 0 for rest"
  [j-hmm prev-state-index]
  (when prev-state-index
    (doseq [i (range (.nbStates j-hmm))]
      (if (= i prev-state-index)
        (.setPi j-hmm i 1)
        (.setPi j-hmm i 0)))))

(defn viterbi-db [data-as-symbols db-name prev-state]
  (let [db-hmms (get-hmms-from-db db-name)]
    (when-not (empty? db-hmms)
      ;(println "data symbols" data-as-symbols)
      (let [hmm (hmm/make-model (first db-hmms))
            j-hmm (:j-hmm hmm)]
        (update-pi j-hmm prev-state)
        (println "Check Pi" j-hmm)
        (apply-viterbi data-as-symbols j-hmm hmm)))))

(defn combine-parts
  "Attempt to do inverse of partition as used below" [n col]
  (into (reduce (fn [result part]
                  (into result (take n part)))
                [] (butlast col)) (last col)))

(defn test-viterbi-db [vit-interval vit-sample-size]
  (let [data-objs (get-observations-samples mdb-name collection-name)
        model (get-hmms-from-db mdb-name)
        model (hmm/make-model model)
        both (split-observations data-objs)
        data-2nd-half (:test-data both)
        data-parts (partition vit-sample-size vit-interval data-2nd-half)
        ;data-flat            (flatten data-parts)
        data-transition-ints (into [] (hmm/to-transition-indices model (extract-observations data-2nd-half :chosen-coin)))
        data-transition-sym (into [] (extract-observations data-2nd-half :chosen-coin))
        ;data-sym      (extract-observations data-2nd-half :observed-face)
        vit-learned (reduce (fn [result sample]
                              (println "Last result:" (last (last result)))
                              (conj result (viterbi-db (extract-observations sample :observed-face) mdb-name (last (last result)))))
                            [] data-parts)
        vit-flat (combine-parts vit-interval vit-learned)]   ;(into [] (flatten vit-learned))

    ;(viterbi-db data-sym mdb-name)

    ;(println "Data flat:" (count data-flat) (count data-transition-ints))
    ;(println "Vit flat:" (count vit-flat))
    (println "n,viterbi,ground truth, ground truth symbols," "Interval:" vit-interval ",Sample Size:" vit-sample-size)
    (dotimes [n (count vit-flat)]
      (println (inc n) "," (get vit-flat n) ","
               (get data-transition-ints n) ","
               (get data-transition-sym n)))))


(defn bw-viterbi [vit-frequency vit-sample-size]
  (let [all-data (get-observations-samples mdb-name collection-name)
        both (split-observations all-data)
        sample-data (:sample-data both)
        test-data (into [] (:test-data both))
        coin-model (hmm/make-model pamela-model)
        j-learned (learn-bw (extract-observations sample-data :observed-face) coin-model bwiterations)
        vit-samples (partition vit-sample-size vit-frequency test-data)
        vit-samples-flat (flatten vit-samples)
        vit-samples-ints (into [] (hmm/to-transition-indices coin-model (extract-observations vit-samples-flat :chosen-coin)))
        vit-learned (reduce (fn [result sample]
                              (conj result (apply-viterbi (extract-observations sample :observed-face) j-learned coin-model)))
                            [] vit-samples)
        _ (println "Vit learned" (count vit-learned))
        vit-learned (into [] (flatten vit-learned))]


    ;(apply-viterbi (extract-observations test-data :observed-face) j-learned coin-model)
    (println "partition count:" (count vit-samples))
    (println "Vit learned flatten" (count vit-learned))
    (dotimes [n (count vit-learned)]
      (println (inc n) "," (get vit-learned n) ","
               (get vit-samples-ints n)))
    (println "java hmm learned" j-learned)))
    ;(doseq [part (partition vit-lookback vit-frequency test-data)]
    ;  (println (count part) part )
    ;  (println (apply-viterbi (extract-observations part :observed-face) j-learned coin-model))
    ;  )


; Version that applies viterbi to all samples and prints CSV
(defn test-bw-from-db-2 []
  (let [all-samples (get-observations-samples mdb-name collection-name)
        both (split-observations all-samples)
        sample-data (:sample-data both)
        test-data (into [] (:test-data both))

        hmm pamela-hmm
        sample-data-symbols (extract-observations sample-data :observed-face)
        test-data-int (hmm/to-emission-indices hmm (extract-observations test-data :observed-face))
        test-data-states-int (into [] (hmm/to-transition-indices hmm (extract-observations test-data :chosen-coin)))

        learned (learn-bw sample-data-symbols
                          hmm bwiterations)
        viterbi (ViterbiCalculator. (to-integer-observations test-data-int) learned)
        vit-states-int (into [] (map int (.stateSequence viterbi)))
        vit-states (into [] (hmm/from-transition-indices hmm vit-states-int))]

    (pprint {
             :all-samples   (count all-samples)
             :sample-data-2 (take 2 sample-data)
             :test-data-2   (take 2 test-data)
             :learned       learned})

    ;(println "Viterbi" viterbi)
    (println "state sequence" (take 10 vit-states))
    (dotimes [n (count test-data)]
      (println (+ n 1) "," (get vit-states-int n) ","
               (get test-data-states-int n) ","

               (name (get vit-states n)) ","
               (:chosen-coin (get test-data n))))))



; Previous version that extracted observations only
(defn test-bw-from-db
  "Read data from Mongo and produce learned parameters"
  []
  (let [observations (get-observations mdb-name collection-name observation-key)
        ;_            (println (count observations) observations)
        as-ints (hmm/to-emission-indices pamela-hmm observations)
        both (split-observations as-ints)
        sample-data (:sample-data both)
        test-data (:test-data both)
        obs-hmm (integer-observations-from-2d-array (list sample-data))
        _ (println "BW Iterations" (.getNbIterations bw))
        _ (.setNbIterations bw bwiterations)
        _ (println "BW Iterations" (.getNbIterations bw))
        j-learned (.learn bw (:j-hmm pamela-hmm) obs-hmm)]


    (pprint {:int-obs      (count as-ints)
             :sample-count (count sample-data)
             :test-data    (count test-data)
             :obs-hmm      (count obs-hmm)
             :j-learned    j-learned})

    #_(loop [counter 0]
        (if (= counter (count test-data))))

    (dotimes [n (count test-data)]
      (println (take n test-data)))))


