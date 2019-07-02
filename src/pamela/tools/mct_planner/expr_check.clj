;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.expr-check
  (:require [pamela.tools.mct-planner.expr]
            [pamela.tools.mct-planner.util :as util]
            [pamela.tools.utils.util :as tutil]
            [clojure.pprint :refer :all]))

; Initially implemented to support in-range constraints check but used elsewhere as well.
(defn in-range [x constraint]
  "To check 'x' is within range of 'contraint'
  perform contraint - x,
  so delta-ub >= 0 implies ub is in range.
     delta-lb <= 0 implies lb is in range."
  (let [diff (util/bounds-diff constraint x)]
    ;(println "bounds-diff, constraint:" constraint " - x:" x " = " diff)
    (and (>= 0 (first diff))
         (>= (second diff) 0))))

(defn get-max-bounds [constraints]
  "Given a list of bounds, return the bound with highest UB"
  (reduce (fn [x y]
            (cond (and (number? (second x)) (number? (second y)))
                  (if (>= (second x) (second y))
                    x y)

                  (= :infinity (second x))
                  x

                  (= :infinity (second y))
                  y

                  :else
                  (binding [*out* *err*]
                    (println "get-max-temporal-constraint Invalid temporal constraint in " constraints))))
          constraints))

(defn get-max-with-infinity [x y]
  ;(println "get-max-with-infinity" x y)
  (cond (and (number? x) (number? y))
        (max x y)

        (or (= :infinity x) (= :infinity y))
        :infinity
        :else
        (binding [*out* *err*]
          (println "get-max-with-infinity cannot find max between" x y))))

(defn get-min-max-bounds [bounds]
  "Given a list of bounds, ([1 11] [2 12] [3 13]), return bounds [lb ub]
  where lb is minimum of all lbs and ub is maximum of all ubs."
  (reduce (fn [x y]

            (let [xb (util/to-bounds x)
                  yb (util/to-bounds y)
                  lb (min (first xb) (first yb))
                  ub (get-max-with-infinity (second xb) (second yb))]
              (println "get-min-max-bounds" xb yb "=" [lb ub])
              [lb ub]
              ))
          bounds))

; Notes: Expression value is the value assigend to the expression. It is the value of the to-var.
; If the to-var is not bound, then the value for the to-var is either propagated or
; chosen, ex a random value for bounds. So, in the case of unbound var, the to-var is bound to the value and
; also the expression is bound to the same value
; If the to-var is already bound, then only expression is bound to the value.
;
;So when checking constraints,

;(defmulti satisfies-constraint?
;          "Given an expression, check if the value satisfies the constraint"
;          (fn [expr value bindings]
;            (println "type " (type (:symbol expr)))
;            (:symbol expr)))
;
;(defmethod satisfies-constraint? nil [expr value bindings]
;  (util/to-std-err (println "bad expr:" expr))
;  false)
;
;(defmethod satisfies-constraint? :default [expr value bindings]
;  (util/to-std-err (println "Impl satisfies-constraint? for expr type" (:symbol expr) (type (:symbol expr))))
;  false)
;
;(defmethod satisfies-constraint? 'cost= [expr value bindings]
;  (= value (:value expr)))
;
;(defmethod satisfies-constraint? 'reward= [expr value bindings]
;  (= value (:value expr)))
;
;(defmethod satisfies-constraint? 'in-range [expr value bindings]
;  ;(println "in-range constraint" expr)
;  (in-range value (:value expr))                            ;note given value is the bound value for the constraint expression
;  )

(defn check-bindings [expr bindings]
  (if-not (pamela.tools.mct-planner.expr/vars-bound? expr bindings)
    (println "Warning: All vars not bound for Expr:" expr)))

(defmulti bindings-satisfy-expr
          "Given an expression and bindings, returns true if the bindings satisfy the
          expression or false"
          (fn [expr bindings]
            (:symbol expr)))

(defmethod bindings-satisfy-expr nil [expr bindings]
  (tutil/to-std-err (println "Warning: bindings-satisfy-expr nil")
                      (tutil/check-type pamela.tools.mct_planner.expr.expr expr))
  false)

(defmethod bindings-satisfy-expr :default [expr _]
  (binding [*out* *err*]
    (println "Impl bindings-satisfy-expr for" (:symbol expr) expr)
    false))

; How to verify bindings satisfy constraint?
; consider range constraint 'c' between nodes 'b' (to node) and 'a' (from node)
; We want to ask, does b - a  satisfies constraint c
; i.e b - a sats? c . This can be rewritten as
; b - c ( = x) sats? a. Criteria of sats? changes with every expression
; For this case, it is x.lb >= a.lb and x.ub <= a.ub
; Unfortunately, it breaks for the case [0 Infinity]  -  [0 0] sats?  [0 Infinity]
; b - c = x is [0 Infinity]
;[0 Infinity] sats? [0 Infinity]
;Lower bound check 0 >= 0 is true
;Upper bound check Infinity <= 0 is false

#_(defn in-range-helper-does-not-work [b a c]
  (println "in-range-helper" b a c)
  (let [x (util/bounds-diff b c)
        a (util/to-bounds a)
        sats? (and (>= (first x) (first a))
                   (<= (second x) (second a)))]
    (println "b - c = x is" x)
    (println x "sats?" c)
    (println "Lower bound check" (first x) ">=" (first a) "is" (>= (first x) (first a)))
    (println "Upper bound check" (second x) "<=" (second a) "is" (<= (second x) (second a)))
    (println "Constraint check:" b " - " a "sats? " c "=>" sats?)
    sats?))

; Another way we could check is by rewriting the expression as
; b sats? a + c ( = x)
; In this case we check for constraint satisfaction as
;  b.lb >= x.lb and b.ub <= x.ub
; Note; b is the bound value and x is the expected value
(defn in-range-helper [b a c]
  ;(println "in-range-helper" a "+" c "=" b)
  (let [a (util/to-bounds a)
        b (util/to-bounds b)
        c (util/to-bounds c)
        x [(+ (first a) (first c)) (+ (second a) (second c))]
        sats? (and (>= (first b) (first x))
                   (<= (second b) (second x)))]
    ;(println "a + c = x is" x)
    ;(println b "sats?" x)
    ;(println "Lower bound check" (first b) ">=" (first x) "is" (>= (first b) (first x)))
    ;(println "Upper bound check" (second b) "<=" (second x) "is" (<= (second b) (second x)))
    ;(println "Constraint check:" b " - " a "sats? " c "=>" sats?)
    sats?))

(defmethod bindings-satisfy-expr 'in-range [expr bindings]
  (check-bindings expr bindings)
  ;(println "bindings-satisfy-expr 'in-range" expr)
  ;(pprint expr)
  (in-range-helper (get bindings (:to-var expr))
                   (get bindings (first (:from-vars expr)))
                   (:value expr)))

(defmethod bindings-satisfy-expr 'range<= [expr bindings]
  ;(println "bindings-satisfy-expr 'range<=")
  ;(println expr)
  ;(pprint bindings)
  (let [to-var (util/to-bounds (get bindings (:to-var expr)))
        from-var (util/to-bounds (get bindings (first (:from-vars expr))))]
    #_(println "To and from vars" to-var from-var)
    (and (<= (first to-var) (first from-var))
         (<= (second to-var) (second from-var)))))

(defmethod bindings-satisfy-expr 'in-range-max [expr bindings]
  ;(println "bindings-satisfy-expr 'in-range-max")
  ;(pprint expr)
  ;(pprint bindings)
  (check-bindings expr bindings)
  ;(println "from-vars" (:from-vars expr))
  (let [bounds (pamela.tools.mct-planner.expr/from-values expr bindings)
        max (util/get-max-max-bounds bounds)
        result (in-range max (get bindings (:to-var expr)))]
    ;(println "bounds" bounds)
    ;(println "max" max)
    ;(println "result" result)
    result))

(defmethod bindings-satisfy-expr 'cost-max [expr bindings]
  (check-bindings expr bindings)
  #_(util/to-std-err (println "Warning: Verify:- Expr" expr)
                     (pprint bindings))
  #_(let [vals (from-values expr bindings)
          _ (println "from-values" vals)])
  (>= (get bindings (:to-var expr)) (apply max (pamela.tools.mct-planner.expr/from-values expr bindings))))

(defmethod bindings-satisfy-expr 'reward-max [expr bindings]
  (check-bindings expr bindings)
  #_(util/to-std-err (println "Warning: Verify:- Expr" expr)
                     (pprint bindings))
  (>= (get bindings (:to-var expr)) (apply max (pamela.tools.mct-planner.expr/from-values expr bindings))))

(defmethod bindings-satisfy-expr '= [expr bindings]
  (check-bindings expr bindings)
  (let [from-val (first (pamela.tools.mct-planner.expr/from-values expr bindings))
        to-val (get bindings (:to-var expr))]
    (= to-val from-val)))

(defmethod bindings-satisfy-expr 'cost= [expr bindings]
  (check-bindings expr bindings)
  (let [from-val (first (pamela.tools.mct-planner.expr/from-values expr bindings))
        to-val (get bindings (:to-var expr))]
    (= (:value expr) (- to-val from-val))))

(defmethod bindings-satisfy-expr 'reward= [expr bindings]
  (check-bindings expr bindings)
  (let [from-val (first (pamela.tools.mct-planner.expr/from-values expr bindings))
        to-val (get bindings (:to-var expr))]
    (= (:value expr) (- to-val from-val))))

(defmethod bindings-satisfy-expr 'selector= [expr bindings]
  ;(check-bindings expr bindings); choice of activity for choice does not depend on any of from-vars to be bound.

  (let [from (select-keys bindings (:from-vars expr))
        fcount (count from)]
    (if (not= 0 fcount)
      (tutil/to-std-err
        (println "bindings-satisfy-expr 'selector=")
        (println "expr has non 0 bindings for from-vars:" from)
        (println expr)
        (println bindings))))

  ; to-var value should be one of the from-vars.
  (contains? (:from-vars expr) ((:to-var expr) bindings)))

(defmethod bindings-satisfy-expr 'if= [expr bindings]
  (check-bindings expr bindings)
  (if (util/if=chosen expr bindings)
    (= ((:to-var expr) bindings) ((first (:from-vars expr)) bindings))
    true                                                    ;This path was not taken so it does not contribute towards the failure case. return true.
    ))

;-----------
(defmulti normalize-expr-value
          "Given an expression and bindings, return (- to-var from-var)"
          (fn [expr bindings]
            (:symbol expr)))

(defmethod normalize-expr-value nil [expr bindings]
  (tutil/to-std-err (println "Warning: normalize-expr-value nil")
                    (tutil/check-type pamela.tools.mct_planner.expr.expr expr))
  nil)

(defn default-normalize-expr-value [expr bindings]
  (- (get bindings (:to-var expr)) (get bindings (first (:from-vars expr)))))

(defmethod normalize-expr-value :default [expr bindings]
  (tutil/to-std-err (println "Warning: No impl normalize-expr-value " (:symbol expr))
                    ; FIXME class not found error with boot check-errors
                    #_(tutil/check-type pamela.tools.mct-planner.expr.expr expr)
                    (println "Assuming default: to-var - from-var "))
  (default-normalize-expr-value expr bindings))

(defmethod normalize-expr-value 'max-cost [expr bindings]
  (default-normalize-expr-value expr bindings))

(defmethod normalize-expr-value 'min-reward [expr bindings]
  (default-normalize-expr-value expr bindings))

(defmethod normalize-expr-value 'in-range [expr bindings]
  (let [expr-value (:value expr)
        actual-time (util/bounds-diff ((:to-var expr) bindings) ((first (:from-vars expr)) bindings))
        constraint-diff (util/bounds-diff actual-time expr-value)
        lb-ok? (>= (first constraint-diff) 0)
        ub-ok? (>= 0 (second constraint-diff))]
    [(if lb-ok? (first expr-value) (first actual-time))
     (if ub-ok? (second expr-value) (second actual-time))]))

(defmethod normalize-expr-value 'range<= [expr bindings]
  (util/bounds-diff ((:to-var expr) bindings) ((first (:from-vars expr)) bindings)))

; Another intermediate implementation leaving here for reference. 2/23/2016 -- PM
#_(defn bounds-diff [x y]
    "x and y could be a number or bounds vector. If number then they are converted to bounds as [x x].
     Lower bound of x and y must be a number.
     Upper bound of x and y could be a number or :infinity. Anything else is error
     returns x - y.
     Note: Only following cases will return a bound and nil otherwise
           number - number
           :infinity - ? = :infinity
           number - :infinity = :-infinity"

    (let [x-bounds (repr.util/to-bounds x)
          y-bounds (repr.util/to-bounds y)
          lb-diff (- (first x-bounds) (first y-bounds))

          xu (second x-bounds)
          yu (second y-bounds)]

      (cond (= xu java.lang.Double/POSITIVE_INFINITY)
            [lb-diff java.lang.Double/POSITIVE_INFINITY]

            (and (number? xu) (number? yu))
            [lb-diff (- xu yu)]

            ;(= :infinity xu); old way. Now we use java Infinity
            ;[lb-diff :infinity]
            ; We should never reach here. PM 2/20/2017
            (number? xu)
            (do
              (binding [*out* *err*]
                (println "WARN: UB will be -infinity. Trying to " xu " - " yu))
              [lb-diff :-infinity])
            :else
            (binding [*out* *err*]
              (println "ERROR: bounds-diff cannot do" x "-" y)))))
