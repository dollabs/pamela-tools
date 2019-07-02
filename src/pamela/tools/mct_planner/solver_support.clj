;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.solver-support
  (:require [pamela.tools.mct-planner.expr :as expr]
            [pamela.tools.mct-planner.expr-check :as expr_check]
            [pamela.tools.mct-planner.util :as ru]
            [pamela.tools.mct-planner.learning :as mlearn]
            [pamela.tools.mct-planner.weighted-probability :as wp]
            [pamela.tools.utils.util :as util]

            [clojure.pprint :refer :all]
            [clojure.core.matrix :as mat]

            ))

(def with-learning false)
(def with-static-uncertainty false)
(def debug true)

(defn print-learning-algos []
  (println "solver-support - with-static-uncertainty =" with-static-uncertainty "with-learning =" with-learning))

(defn set-choice-algo [static-unc? learning?]
  (def with-static-uncertainty static-unc?)
  (def with-learning learning?)
  (print-learning-algos))

(def expr-enabled-hierarchy (-> (make-hierarchy)

                                (derive 'reward= 'cost=)

                                (derive 'cost-max 'in-range-max)
                                (derive 'reward-max 'in-range-max)

                                (derive 'range<= 'in-range)
                                (derive 'cost<= 'in-range)
                                (derive 'reward<= 'in-range)))

(defmulti expr-enabled?
          "An expr is enabled if any of the vars could be bound / propagated"
          (fn [expr bindings]
            (:symbol expr))
          :hierarchy #'expr-enabled-hierarchy)

(defmethod expr-enabled? :default [expr bindings]
  (util/to-std-err
    (println "Warning: Impl: expr-enabled?" (:symbol expr))
    (pprint expr)
    (pprint bindings))
  (throw (Exception. (str "Handle expr-enabled? " (:symbol expr)))))

(defmethod expr-enabled? '= [expr bindings]
  (let [to-bound (expr/to-var-bound? expr bindings)
        from-bound (expr/from-vars-bound? expr bindings)]
    (cond (and to-bound from-bound)
          false
          (or to-bound from-bound)
          true
          :else false
          )))

(defmethod expr-enabled? 'cost= [expr bindings]
  (let [to-bound (expr/to-var-bound? expr bindings)
        from-bound (expr/from-vars-bound? expr bindings)]
    (cond (and from-bound #_(not to-bound) )
          true
          :else false)))

; to-var could be bound for cases like in-between constraints so to-var cannot be used to assert if the
; expression is enabled or not
(defmethod expr-enabled? 'in-range-max [expr bindings]
  ; Can only be used for propagating values when from-vars is bound
  (expr/from-vars-bound? expr bindings))

(defmethod expr-enabled? 'in-range [expr bindings]
  ; Enabling in-range for propagation only in forward direction.
  ; Could be enabled in backward direction as well. Not sure if we need it?
  (expr/from-vars-bound? expr bindings))

(defmethod expr-enabled? 'selector= [expr bindings]
  (not (expr/from-vars-bound? expr bindings)))

(defmethod expr-enabled? 'if= [expr bindings]
  (let [from-select (first (:value expr))
        to-select (second (:value expr))
        from-bound (expr/from-vars-bound? expr bindings)
        to-bound (expr/to-var-bound? expr bindings)]

    (and (= (get bindings from-select) to-select)
         (cond (and to-bound from-bound)
               false
               (or to-bound from-bound)
               true
               :else
               false))))

(defmethod expr-enabled? 'max-cost [expr bindings]
  (expr/from-vars-bound? expr bindings))

(defmethod expr-enabled? 'min-reward [expr bindings]
  (expr/from-vars-bound? expr bindings))

(defn enabled-exprs
  "Return expressions that are enabled"
  [exprs bindings handled]
  (into [] (filter (fn [expr]
                     (cond (and (expr-enabled? expr bindings)
                                (not (contains? handled expr)))
                           true
                           :else
                           false))
                   exprs)) )

;; Propagate bindings for vars ;;;;;;;;;;;;;;;

(def propagate-hierarchy (-> (make-hierarchy)
                             (derive 'reward= 'cost=)
                             (derive 'max-cost 'cost=)
                             (derive 'if= '=)
                             (derive 'cost<= '=)
                             (derive 'reward<= '=)
                             (derive 'reward-max 'cost-max)
                             ))

(defmulti propagate
          "An expression must be enabled
          var(s?) to be propagated must not have a value in bindings
          Return updated bindings when var(s?) are bound.
          Otherwise return same bindings
          Return object is a map of :bindings and :expr-values

          Cases:
          to-var value is not bound
            expr is not controllable
             - propagate and bind the value
            expr is controllable
             - choose a value. bind the value and conj the {expr chosen-value} to expr-values

          to-var value is bound
            expr is not controllable
              no change
            expr is controllable
              expr-value is constrained by it's own constraint and the contraint imposed by the bound value of to-var
          "
          (fn [expr bindings expr-values]
            (:symbol expr))
          :hierarchy #'propagate-hierarchy)

(defmethod propagate :default [expr bindings expr-values]
  (util/to-std-err
    (println "Warning: Impl: propagate" (:symbol expr))
    (pprint expr)
    (pprint bindings))
  (throw (Exception. (str "Handle propagate " (:symbol expr)))))

(defmethod propagate '= [expr bindings expr-values]
  (let [from-var (-> expr :from-vars first)
        from-val (get bindings from-var)
        to-var (-> expr :to-var)
        to-val (get bindings to-var)
        return-obj {:expr-values expr-values :bindings bindings}
        ]
    ;(pprint expr)
    ;(println "propagate " (:symbol expr) from-var from-val to-var to-val)
    (cond (and (not (nil? from-val))
               (nil? to-val))
          (conj return-obj {:bindings (conj bindings {to-var from-val})})
          (and (not (nil? to-val))
               (nil? from-val))
          (conj return-obj {:bindings (conj bindings {from-var to-val})})
          :else
          return-obj)))

(defn bind-controllable-range
  "expr must have monte-learner
  v-lb-ub is [lb ub]
  returns a value from monte-learner or randomly between lb and ub ( inclusive of both lb and ub)"
  [expr v-lb-uv ]
  ;(println "bind-controllable-range" v-lb-uv)
  ;(pprint expr)
  ; We don't add :monte-learner for [0 Infinity] case for performance reasons. FIXME
  (if (and with-learning (contains? (:m expr) :monte-learner))
    (mlearn/sample-from-monte (get-in expr [:m :monte-learner]))
    (ru/random-bound v-lb-uv)))

(defn cost-reward-propagate-helper [expr bindings expr-values bounds]
  (let [from-var (-> expr :from-vars first)
        to-var (-> expr :to-var)
        from-val (get bindings from-var)
        to-val (get bindings to-var)
        return-obj {:expr-values expr-values :bindings bindings}]

    (when (nil? from-val)
      (util/to-std-err
        (println (str "propagate " (:symbol expr) " expected from-val to be valid. Got nil!"))
        (println "expr and bindings")
        (pprint expr)
        (pprint bindings))
      (throw (Exception. (str "propagate " (:symbol expr) " expected from-val to be valid. Got nil!"))))

    (if (expr/controllable? expr)
      (if (nil? to-val)
        (let [chosen-val (bind-controllable-range expr bounds)]
          ;(println "propagate controllable " (:symbol expr) to-var "is" chosen-val from-var "is" from-val  [0 chosen-val (:value expr)])
          (conj return-obj {:bindings (conj bindings {to-var (+ from-val chosen-val)})
                            :expr-values (conj expr-values {expr chosen-val})}))
        (do
          ;to-var is bound. Choose expr-value for the controllable expr.
          ; expr-value is to-var - from-var. It is already chosen for us and we need to learn about this value.
          ;(println "propagate controllable bound" (:symbol expr) to-var to-val from-var from-val  [0 (- to-val from-val) (:value expr)])
          (conj return-obj {:expr-values (conj expr-values {expr (- to-val from-val)})})))
      (let [to-val (get bindings to-var)]
        ;(println "propagate " (:symbol expr) from-var from-val to-var to-val)
        (cond (nil? to-val)
              (conj return-obj {:bindings (conj bindings {to-var (+ from-val (:value expr))})})
              :else
              return-obj)))))

(defmethod propagate 'cost= [expr bindings expr-values]
  (cost-reward-propagate-helper expr bindings expr-values [0 (:value expr)]))

(defmethod propagate 'min-reward [expr bindings expr-values]
  ; Use max-reward from maximum possible reward from constraint propagation
  (cost-reward-propagate-helper expr bindings expr-values [(:value expr) (-> expr :m :propagated :reward-bounds last)]))

(defn controllable-and-learnable [expr]
  (and (expr/controllable? expr) (contains? (:m expr) :monte-learner)))

(defn add-longs-with-overflow [a b]
  (try
    (+ a b)
       (catch ArithmeticException e
         java.lang.Long/MAX_VALUE)))

(defn add-bounds [from to]
  (println "add-bounds from to" from to)
  #_(mat/add from to)
  [(add-longs-with-overflow (first from) (first to))
   (add-longs-with-overflow (second from) (second to))
   ])

(defmethod propagate 'in-range [expr bindings expr-values]
  ;(println "propagate 'in-range" expr)
  ;(pprint bindings)
  ;(pprint expr-values)
  (if-not (expr/to-var-bound? expr bindings)
    (let [from-val (->> expr :from-vars first (get bindings))
          from-bounds (ru/to-bounds from-val)
          val (if (expr/controllable? expr)
                (bind-controllable-range expr (:value expr))
                (:value expr))
          to-bounds (ru/to-bounds val)
          to-var (:to-var expr)
          bindings (try (conj bindings {to-var (mat/add from-bounds to-bounds)})
                        (catch Exception e
                          (println "Got Exception" (.getMessage e))))
          expr-values (if (controllable-and-learnable expr)
                        (conj expr-values {expr val})
                        expr-values)]
      {:expr-values expr-values :bindings bindings})
    (if (expr/controllable? expr)
      ;to-var is bound. Choose expr-value for the controllable expr.
      ; value of the expr is constrained by it's own constraint and by to-var - from-var
      (let [d (ru/bounds-diff (get bindings (:to-var expr)) (get bindings (first (:from-vars expr))))
            constrained [(max (first d) (first (:value expr))) (min (second d) (second (:value expr)))]]
        ;(println (:symbol expr) "controllable and to-var bound has constrains" d (:value expr) "=" constrained)
        {:expr-values (conj expr-values {expr (cond (and with-learning (not (nil? (get-in expr [:m :monte-learner]))))
                                                    (mlearn/sample-from-monte-for-values (get-in expr [:m :monte-learner]) constrained)

                                                    (and with-learning (nil? (get-in expr [:m :monte-learner])))
                                                    (util/to-std-err (println "Monte learner is nil for expr values" (:value expr)
                                                                              "constrained value" constrained)
                                                                     (println "Will use random bounds for this expression")
                                                                     (ru/random-bound constrained))
                                                    :else
                                                    (ru/random-bound constrained))
                                         }) :bindings bindings})
      {:expr-values expr-values :bindings bindings})

    ))

(defmethod propagate 'cost-max [expr bindings expr-values]
  ;(println expr)
  ;(pprint bindings)
  ;(println "propagate 'cost-max" (:from-vars expr) (expr/from-values expr bindings))
  ; Note: This expr is created for p-end node and they are always uncontrollable.
  (if-not (expr/to-var-bound? expr bindings)
    {:expr-values expr-values
     :bindings    (conj bindings {(:to-var expr) (let [vals (expr/from-values expr bindings)]
                                                   (if-not vals
                                                     0
                                                     (apply max vals)
                                                     ))})}
    {:expr-values expr-values :bindings bindings}))

(defmethod propagate 'range<= [expr bindings expr-values]
  ; Note: This expr is created for p-end node and they are always uncontrollable.
  (if (and (not (expr/to-var-bound? expr bindings))
           (expr/from-vars-bound? expr bindings))
    {:expr-values expr-values
     :bindings    (conj bindings {(:to-var expr) (->> expr :from-vars first (get bindings))})}
    {:expr-values expr-values :bindings bindings}))

(defmethod propagate 'in-range-max [expr bindings expr-values]
  ; Note: This expr is created for p-end node and they are always uncontrollable.
  (if-not (expr/to-var-bound? expr bindings)
    {:expr-values expr-values
     :bindings    (conj bindings {(:to-var expr) (ru/get-max-max-bounds (expr/from-values expr bindings))})}
    {:expr-values expr-values :bindings bindings}))


(defmethod propagate 'selector= [expr bindings expr-values]
  (if-not (expr/to-var-bound? expr bindings)
    (let [selected (cond (true? with-static-uncertainty)
                         (wp/sample-weighted-distribution (get-in expr [:m :weighted-distribution]))
                         (true? with-learning)
                         (mlearn/sample-from-monte (get-in expr [:m :monte-learner]))
                         :else
                         (rand-nth (into [] (:from-vars expr))))]
      ;(println selected "=" expr)
      {:expr-values (conj expr-values {expr selected})
       :bindings (conj bindings {(:to-var expr) selected} {selected true})})
    {:expr-values expr-values :bindings bindings}))

;; Assign value to the expr ;;;;;;;;;;;;;;;
#_(def choose-expr-value-hierarchy (-> (make-hierarchy)
                                       (derive 'cost= '=)
                                       (derive 'reward= '=)
                                       (derive 'cost-max '=)
                                       (derive 'reward-max '=)
                                       (derive 'range<= '=)
                                       (derive 'in-range-max '=)
                                       (derive 'selector= '=)
                                       (derive 'if= '=)
                                       (derive 'cost<= '=)
                                       (derive 'reward<= '=)))

#_(defmulti choose-expr-value
            "Assume all the vars of the expression are bound.
            Given these bindings, choose a value for the expression"
            (fn [expr bindings]
              (:symbol expr))
            :hierarchy #'choose-expr-value-hierarchy)

#_(defmethod choose-expr-value :default [expr bindings]
    (util/to-std-err
      (println "Warning: Impl: choose-expr-value" (:symbol expr))
      (pprint expr)
      (pprint bindings))
    (throw (Exception. (str "Handle choose-expr-value " (:symbol expr)))))


#_(defmethod choose-expr-value '= [expr bindings]
    (->> expr :to-var (get bindings)))

#_(defmethod choose-expr-value 'in-range [expr bindings]
    ; range expression value is constrained by to - from vars.
    (let [to-bounds (->> expr :to-var (get bindings) ru/to-bounds)
          from-bounds (->> expr :from-vars first (get bindings) ru/to-bounds)
          d (expr_check/bounds-diff to-bounds from-bounds)
          ]
      (cond (nil? d)
            (do
              (util/to-std-err (println "choose-expr-value 'in-range bounds-diff is nil" to-bounds "-" from-bounds "=" d)
                               (println expr)
                               (pprint bindings))
              bindings)
            :else
            (if (expr/controllable? expr)
              (ru/random-bound d)
              d))))


(defn update-expression-value [expr value ev-m]
  (if-not (contains? ev-m expr)
    (conj ev-m {expr value})
    ev-m))


;; Check if the expr-value satisfies the expression

(def expr-satisfies-contraint?-hierarchy (-> (make-hierarchy)
                                             (derive 'reward= 'cost=)
                                             (derive 'max-cost 'cost=) ; over arching constraint
                                             (derive 'reward-max 'cost-max) ; p-end constraint
                                             (derive 'reward<= 'cost<=))) ; p-end constraint

(defmulti expr-satisfies-contraint?
          "Returns true/false if the expression satisfies constraint or not"
          (fn [expr bindings]
            (:symbol expr))
          :hierarchy #'expr-satisfies-contraint?-hierarchy)

(defmethod expr-satisfies-contraint? :default [expr bindings]
  (util/to-std-err
    (println "Warning: Impl: expr-satisfies-contraint" (:symbol expr))
    (pprint expr)
    ;(println "Value sats? constraint?: " exp-value "sats?" (:value expr))
    (pprint bindings))
  (throw (Exception. (str "Handle expr-satisfies-contraint " (:symbol expr)))))

(defmethod expr-satisfies-contraint? '= [expr bindings]
  (= (get bindings (:to-var expr))
     (get bindings (-> expr :from-vars first))))

(defmethod expr-satisfies-contraint? 'cost= [expr bindings]
  ;(println "expr-satisfies-contraint?" (:symbol expr) "controllable" (expr/controllable? expr))
  (let [to-val (get bindings (:to-var expr))
        from-val (get bindings (-> expr :from-vars first))]
    ;(println to-val from-val ":value" (:value expr))
    (if (expr/controllable? expr)
      (<= (- to-val from-val) (:value expr))
      (= (get bindings (:to-var expr))
         (+ (:value expr) (get bindings (-> expr :from-vars first)))))))

(defmethod expr-satisfies-contraint? 'min-reward [expr bindings]
  (let [to-val (get bindings (:to-var expr))
        from-val (get bindings (first (:from-vars expr)))]
    (>= (- to-val from-val) (:value expr))))

(defmethod expr-satisfies-contraint? 'in-range [expr bindings]

  ;(println "expr" expr)
  ;(pprint bindings)
  (let [to (ru/to-bounds (get bindings (:to-var expr)))
        from (ru/to-bounds (get bindings (first (:from-vars expr))))
        ub (- (second to) (second from))
        lb (- (first to) (first from))]
    ;(println to "-" from "= [" lb ub "]")
    ;(println ">= ub lb" (>= ub lb))
    ;(println "in-range-helper check" (expr_check/in-range-helper to from (:value expr)))
    ;[0 Infinity] - [0 Infinity] = [ 0 NaN ]
    ;>= ub lb false for NAN - 0
    ;in-range-helper check true
    #_(and (>= ub lb) (expr_check/in-range-helper to from (:value expr)))
    ; in-range-helper is handling inf correctly.
    (expr_check/in-range-helper to from (:value expr))))

(defmethod expr-satisfies-contraint? 'range<= [expr bindings]
  (let [to-val (ru/to-bounds (get bindings (:to-var expr)))
        from-val (ru/to-bounds (get bindings (first (:from-vars expr))))]
    (and (<= (first to-val) (first from-val))
         (<= (second to-val) (second from-val)))))

(defmethod expr-satisfies-contraint? 'cost-max [expr bindings]
  (>= (get bindings (:to-var expr)) (let [vals (expr/from-values expr bindings)]
                                      (if-not vals 0
                                                   (apply max vals)))))

(defmethod expr-satisfies-contraint? 'selector= [expr bindings]
  true
  #_(and (contains? (:from-vars expr) exp-value)
       (= true (get bindings exp-value))))

(defmethod expr-satisfies-contraint? 'if= [expr bindings]
  (= (get bindings (:to-var expr))
     (get bindings (-> expr :from-vars first))))

(defmethod expr-satisfies-contraint? 'cost<= [expr bindings]
  (<= (get bindings (:to-var expr)) (get bindings (first (:from-vars expr)))))

(defmethod expr-satisfies-contraint? 'in-range-max [expr bindings]
  (let [max-max (ru/to-bounds (ru/get-max-max-bounds (expr/from-values expr bindings)))
        to-val (ru/to-bounds (get bindings (:to-var expr)))]
    (and (<= (first to-val) (first max-max))
         (<= (second to-val) (second max-max)))))

(defn println-monte-learner-stats [exprs]
  (doseq [expr exprs]
    (when (contains? (get expr :m) :monte-learner)
      (pprint expr)
      (println (mlearn/make-printable (get-in expr [:m :monte-learner]))))))

; Notes
; 'in-range is a constraint
; '= is a used for propagation of var values. This constraint will always be satisfied

; 8/28/2018
; We choose values for controllable only. Rest are simply propagated
; When we choose a value for an expression, then we learn about the success and failure of the expression.
; If we don't choose a value for the expression, there is nothing to be learned.
;