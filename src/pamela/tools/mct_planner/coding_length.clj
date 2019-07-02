;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.coding-length
  (:require [pamela.tools.mct-planner.util :refer :all]
            [pamela.tools.mct-planner.expr :refer :all]
            [pamela.tools.utils.util :as util]
            [clojure.pprint :refer :all]))

; value-code-length is only called for controllable expressions
(defmulti value-coding-length
          "Represents uncertainty in the value part of the expression
           Ex: For bounds, [0 Inf] uncertainty is very high but for
           [5 5], there is no uncertainty."
          (fn [expr]
            (if (false? (:controllable expr))
              (throw (Exception. (str "value-coding-length should only be called for controllable expressions.\n
              expr = " expr "is not controllable = " (:controllable expr))))
              (:symbol expr))))

(defmethod value-coding-length :default [expr]
  (if (controllable? expr)
    (util/to-std-err
      (println "Handle value-coding-length" (:symbol expr))
      (pprint expr)
      (throw (Exception. (str "Handle value-coding-length " (:symbol expr)))))
    (util/to-std-err (println "Warning: Assuming coding-length for uncontrollable
    expr to be 0. Impl value-coding-length" (:symbol expr) expr)))
  0)

(defmethod value-coding-length 'in-range [expr]
  (code-length (probability-distribution (:value expr))))

(defmethod value-coding-length 'cost= [expr]
  (code-length (probability-distribution [0 (:value expr)])))

(defmethod value-coding-length 'reward= [expr]
  (code-length (probability-distribution [0 (:value expr)])))

; 'max-cost is over arching constraint
(defmethod value-coding-length 'max-cost [expr]
  (code-length (probability-distribution [0 (:value expr)])))

; 'min-reward is over arching constraint
(defmethod value-coding-length 'min-reward [expr]
  ;(println "Value coding length reward>=" expr [(:value expr) java.lang.Double/POSITIVE_INFINITY])
  (code-length (probability-distribution [(:value expr) java.lang.Double/POSITIVE_INFINITY])))

(defmulti expr-code-length
          "Given a expression and coding length of fromvars, return coding length for the expr.
          Return a {expr code-length} for the given expression."
          (fn [expr var-cls]
            (:symbol expr)))

(defn expr-code-length-default
  "For uncontrollable expressions, there is full certainty, so value-coding-length is 0
   For controllable expressions, we compute value-coding-length for each expr type and
   compute expr-code-length as
   expr-code-length = value-coding-length + coding-length of from-vars"
  [expr var-cls]
  (let [from-cl (from-values expr var-cls)
        value-cl (if (and (:controllable expr) (:value expr))
                   (value-coding-length expr)
                   0)
        cl (conj from-cl value-cl)
        ]
    (reduce + cl)))

(defmethod expr-code-length :default [expr var-cls]
  (util/to-std-err (println "Warning: Impl: expr-code-length" (:symbol expr) expr))
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'selector= [expr _]
  "This epxression represents choice case where each from-var represents a choice path."
  (code-length (/ 1 (count (:from-vars expr)))))

(defmethod expr-code-length 'in-range [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'if= [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'cost= [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'max-cost [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'reward= [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'min-reward [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'in-range-max [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'cost-max [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'reward-max [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length '= [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'range<= [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'cost<= [expr var-cls]
  (expr-code-length-default expr var-cls))

(defmethod expr-code-length 'reward<= [expr var-cls]
  (expr-code-length-default expr var-cls))