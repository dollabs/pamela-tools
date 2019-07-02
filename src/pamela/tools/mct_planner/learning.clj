;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.learning
  ;defrecords need to be imported because clojure creates java classes for them
  #_(:import [repr.sample_i sample])

  (:require [pamela.tools.mct-planner.util :as ru]
            [pamela.tools.utils.util :as util]
    [clojure.pprint :refer :all]

            [incanter.stats :as is]))

(def optimize-time true)
(def optimize-cost true)
(def optimize-reward true)

(defprotocol monte-learner-i
  (get-pdf-a [monte-learner])
  (get-pdf-a-mass [monte-learner])
  (get-pdf-a-val [this val])
  (get-pdf-a-as-vec [this idx])
  (get-pdf-a-mass-as-vec [this idx])
  (update-pdf-a [monte-learner a])
  (update-pdf-a-mass [monte-learner a])
  (get-b-for-val [monte-learner val])
  (get-b-for-val-val [monte-learner val val2])
  (is-value-valid? [monte-learner value])
  )

(defrecord monte-learner [values storage]
  ; values is a vector of integer values and storage is atom of {}
  monte-learner-i
  (get-pdf-a [_]
    (last (get-in @storage [:pdf-a])))
  (get-pdf-a-mass [_]
    (last (get-in @storage [:pdf-a-mass])))

  (get-pdf-a-val [this val]
    (get (get-pdf-a this) val))
  (get-pdf-a-as-vec [this idx]
    (let [pdf (if idx
                (nth (get-in @storage [:pdf-a]) idx)
                (get-pdf-a this))]
      (reduce (fn [res val]
                (conj res (get pdf val))) [] values)))
  (get-pdf-a-mass-as-vec [this idx]
    (let [enum-dist (if idx
                (nth (get-in @storage [:pdf-a-mass]) idx)
                (get-pdf-a-mass this))
          pdf-mass (reduce (fn [res x]
                             (conj res {(.getKey x) (.getValue x)}))
                           {} (.getPmf enum-dist))]
      (reduce (fn [res val]
                (conj res (get pdf-mass val))) [] values)))

  (update-pdf-a [_ a]
    (swap! storage update-in [:pdf-a] conj a))

  (update-pdf-a-mass [monte-learner a]
    (swap! storage update-in [:pdf-a-mass] conj (ru/make-enum-distribution a)))

  (get-b-for-val [monte-learner val]
    (get-in monte-learner [:pdf-b-all val]))
  (get-b-for-val-val [monte-learner val val2]
    (get-in monte-learner [:pdf-b-all val val2]))
  (is-value-valid? [monte-learner value]
    (contains? (:vals-set monte-learner) value))
  )

(defn count-of-pdfs [m-learner]
  ; We keep track of all pdfs as they are evolved over time. ; see how we 'get-pdf-a-mass-as-vec'  via index
  (count (get @(get m-learner :storage) :pdf-a-mass)))

(defn make-printable [m-learn]
  ;(println "m-learn keys" (keys @(:storage m-learn)))
  {:values (if-not (= :choice (get-in m-learn [:config-options :pdf-for]))
             [(apply min (:values m-learn)) :to (apply max (:values m-learn))]
             (:values m-learn))
   :sample-count (get-in @(:storage m-learn) [:sample-count])
   :update-count (get-in @(:storage m-learn) [:update-count])
   :success-count (get-in @(:storage m-learn) [:success-count])
   :fail-count (get-in @(:storage m-learn) [:fail-count])
   }
  #_{:weights  (:weights weights)                             ;removes reference to object type
   :prob-obj (reduce (fn [res x]

                       (conj res [(.getKey x) (.getValue x)]))
                     [] (.getPmf (:prob-obj weights)))})

(defmethod print-method monte-learner [wd ^java.io.Writer w]
  (.write w (pr-str (make-printable wd))))

(defn pprint-monte-learner [weights]
  (pprint (make-printable weights)))

(. clojure.pprint/simple-dispatch addMethod monte-learner pprint-monte-learner)

; Constants

;number of standard deviations around min and max values to address unexpected issue at edges
;(def nsd 2)
; standard deviation for b
;(def b-sd 1.75)

; lambda for success
;(def lambda-succ (double #_1.0 (/ 1 100)))
;(def divisor 1000)
; lambda value for failure
;(def lambda-fail (/ lambda-succ divisor))
;; For lambda-success 0.01, divisor 50 and 100,000 iterations, learning works.
; Lowest possible probability for any value
;(def lowest-value (double (/ 1 1000)))

(defn get-pdf-normal-as-map [vals-vec point sd]
  ;(println vals-vec)
  ;(println point sd)
  (let [pdf (into [] (is/pdf-normal vals-vec :mean point :sd sd))]
    (loop [idx 0
           result (sorted-map)]
      (if (== idx (count vals-vec))
        result
        (recur (inc idx) (conj result {(get vals-vec idx)
                                       (get pdf idx)}))))))

(defn make-default-mlearner-options []
  ;(println "get-options" options-mp)
  ; TODO Use values from long-learner
  {:b-sd-s 2 :b-sd-f 2
   :lambda-s 1/100 :lambda-f 1/10000
   :lowest-pdf 1/200})

(defn make-b-pdf-for-choice [vals]
  ; for each value of the val, return {v1 {v1 1 v2 0 v3 0}}
  ; Here vX represents a chosen path. If a path is chosen, then probability of other chosen paths is 0
  (reduce (fn [res val]
            (conj res {val (reduce (fn [res2 val2]
                                     (if (= val val2)
                                       (conj res2 {val2 1})
                                       (conj res2 {val2 0})))
                                   {} vals)}))
          {} vals))

(defn make-monte-learner [vals-vec options-mp]
  {:pre [(if (or (nil? options-mp)
                 (empty? options-mp))
           true
           (and (-> options-mp :lambda-s)
                (-> options-mp :lambda-f)
                (-> options-mp :lowest-pdf)
                (-> options-mp :b-sd-s)
                (-> options-mp :b-sd-f)))]}
  ;(println "make-monte-learner pdf-for: " (:pdf-for options-mp))
  ;(pprint options-mp)
  (let [options (if (or (nil? options-mp)
                        (empty? options-mp))
                  (make-default-mlearner-options)
                  options-mp)
        ;min-val (apply min vals-vec)
        ;max-val (apply max vals-vec)
        ;b-sd-success 3
        ;b-sd-failure 1
        ;left-of-min (ru/find-points-around-val min-val nsd)
        ;right-of-max (ru/find-points-around-val max-val nsd)
        ;vals-vec (into vals-vec (:left-side left-of-min))
        ;vals-vec (into vals-vec (:right-side right-of-max))
        initial-prob (double (/ 1 (count vals-vec)))
        pdf-a (reduce (fn [res val]
                        (conj res {val initial-prob})) (sorted-map) vals-vec)
        pdf-for-choice (= :choice (:pdf-for options))
        ;b-for-vals (reduce (fn [res v]
        ;                     (conj res {v (get-pdf-normal-as-map vals-vec v b-sd)}))
        ;                   (sorted-map) vals-vec)
        b-success (if-not pdf-for-choice (reduce (fn [res v]
                                            (conj res {v (get-pdf-normal-as-map vals-vec v (:b-sd-s options))}))
                                          (sorted-map) vals-vec)
                                         (make-b-pdf-for-choice vals-vec)
                                         )
        b-failure (if-not pdf-for-choice (reduce (fn [res v]
                                            (conj res {v (get-pdf-normal-as-map vals-vec v (:b-sd-f options))}))
                                          (sorted-map) vals-vec)
                                         (make-b-pdf-for-choice vals-vec))

        obj (->monte-learner vals-vec (atom {:pdf-a [pdf-a]
                                             :pdf-a-mass [(ru/make-enum-distribution pdf-a)]
                                             }))
        ]
    (conj obj {:pdf-b-success b-success :pdf-b-failure b-failure
               :config-options options
               :vals-set (into #{} vals-vec)
               ;:pdf-b-all b-for-vals
               })))

(defn get-pdf-from-mass [enu-dist]
  (reduce (fn [res x]
            (merge res {(.getKey x) (.getValue x)})
            )
          (sorted-map) (.getPmf enu-dist)))

(defn sample-from-monte [m-learner]
  (let [; pdf-a (get-pdf-a m-learner)
        ;a-dist (ru/make-enum-distribution pdf-a)
        a-dist (get-pdf-a-mass m-learner)]
    ;(println "pdf-a" (print-pdf-mass a-dist))
    (swap! (:storage m-learner) update-in [:sample-count] (fnil inc 0))
    (.sample a-dist )))

(defn sample-from-monte-for-values [m-learner values]
  ; we want to return a value only from the given values and their probabilities from givem m-learner
  (let [enum-dist (get-pdf-a-mass m-learner)
        as-kv (reduce (fn [res x]
                        (conj res {(.getKey x) (.getValue x)}))
                      {} (.getPmf enum-dist))
        vals-dist (select-keys as-kv values)
        vals-obj (ru/make-enum-distribution vals-dist)
        ret (.sample vals-obj)
        ]
    #_(println "sample-from-monte-for-values " ret ", [" values "]")
    ret))

(defn adjust-pdf [a b lambda]
  (reduce (fn [res point]
            ;(println "a + lambda * b" point (get a point) (* lambda (get b point)))
            ;(println "a" (get a point) (type lambda) (type (get b point)))
            (let [val (+ (get a point) (* lambda (get b point)))
                  ;val (if (> val 0)
                  ;      val lowest-value)
                  ]
              (conj res {point val})))
          (sorted-map) (keys a)))

(defn shift-pdf-to-zero [pdf-m lowest-value]
  (let [minn (apply min (vals pdf-m))

        ]
    (if (< minn lowest-value)                                          ;min < 0
      (let [to-shift (- lowest-value minn)]
        ;(println "shifting vals" minn const to-shift)
        (reduce (fn [res [key val]]
                  (conj res {key (+ val to-shift)})) {} pdf-m))
      pdf-m)))

(defn make-range-value-optimize-fn
  "Return a fn that given min and max values, a point p within [mn and mx],

  scale mn, p and mx to translated and return point corresponding
  to p in translated scale

  ex: For [1 2 3 4] will have values [0.2 0.13 0.067 0]
  "
  [translate-min translate-max]
  (fn [mn mx value]
    (-> value (- mn) (/ (- mx mn)) (* (- translate-max translate-min)) (+ translate-min))))

(def optimize-time-helper (make-range-value-optimize-fn 0.2 0))
(def optimize-reward-helper (make-range-value-optimize-fn 0 0.2))

(defn get-lambda [m-learner succ mn mx value optimize-key]
  (let [lambda-p (if succ
                 (get-in m-learner [:config-options :lambda-s])
                 (get-in m-learner [:config-options :lambda-f]))
        lambda (cond (= mn mx)                              ;not much to learn if they are equal
                     lambda-p
                     (and succ optimize-time mn mx (= :time optimize-key))
                     (+ lambda-p (optimize-time-helper mn mx value))
                     (and succ optimize-cost mn mx (= :cost optimize-key))
                     (+ lambda-p (optimize-time-helper mn mx value)) ; same as time. optimize lower values
                     (and succ optimize-reward mn mx (= :reward optimize-key))
                     (+ lambda-p (optimize-reward-helper mn mx value))
                     :else
                     lambda-p)]
    lambda))

(defn update-monte
  "m-learner is the monte-learner record
  value is one of the value for which we are learning. Note that the value can be non numeric
  succ is true or false advising how to learn for the value
  mn and mx indicate that values of m-learner are numeric and are min and max of the values.
    mn and mx are needed for optimizing learning for lower values of temporal bounds
    mn and mx are needed for optimizing learning for lower values of cost
    mn and mx are needed for optimizing learning for higher values of reward
  optimize-key can :time or :reward or :cost
  "
  [m-learner value succ & [mn mx optimize-key]]
  {:pre [(-> m-learner :config-options :lambda-s)
         (-> m-learner :config-options :lambda-f)
         (-> m-learner :config-options :lowest-pdf)]}
  ;(println "update for value" value succ)
  (if (is-value-valid? m-learner value)
    (let [lambda (get-lambda m-learner succ mn mx value optimize-key)
          ;b (get-in m-learner [:pdf-b-all value])
          b (if succ
              (get-in m-learner [:pdf-b-success value])
              (get-in m-learner [:pdf-b-failure value]))

          a (get-pdf-a-mass m-learner)
          updated (adjust-pdf (get-pdf-from-mass a)  b lambda)
          updated (shift-pdf-to-zero updated (get-in m-learner [:config-options :lowest-pdf]))
          ]
      ;(println "updated" updated)
      (update-pdf-a-mass m-learner updated)
      (swap! (:storage m-learner) update-in [:update-count] (fnil inc 0))
      (if (true? succ)
        (swap! (:storage m-learner) update-in [:success-count] (fnil inc 0))
        (swap! (:storage m-learner) update-in [:fail-count] (fnil inc 0)))
      ;(println m-learner)
      #_(if (true? succ)
          (println "update for value" value "success")
          (println "update for value" value "Fail\n"))
      updated)
    (do (util/to-std-err #_(println "update-monte value outside of range:" value m-learner )))
    ) )

