;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.

(ns pamela.tools.mct-planner.planner
  ;(:gen-class)
  (:require [clojure.pprint :refer :all]
            [pamela.tools.mct-planner.solver :as solver]
            [pamela.tools.mct-planner.expr :as expr]
            [pamela.tools.mct-planner.util :as ru]
            [clojure.walk :as w]))

(defn filter-temporal-choice-bindings [expr-var-bindings]
  (reduce (fn [res [varid value]]
            (conj res (if (or (ru/is-range-var? varid)
                              (ru/is-select-var? varid))
                        {varid value}
                        {}))) {} expr-var-bindings))


(defn var-to-node-bindings [var-bindings var-2-nid-lu]
  ;(println "var-bindings")
  ;(pprint var-bindings)
  ;(println "var-2nid-lu")
  ;(pprint var-2-nid-lu)
  (reduce (fn [res [key val]]
            ;(pprint res)
            ;(println key val)
            ;(println)
            (let [sel-val (if (and (ru/is-select-var? key) (get var-2-nid-lu val))
                            (get var-2-nid-lu val))]
              ;(println "sel-val" sel-val)
              (cond sel-val (assoc-in res [(get var-2-nid-lu key) :to-node] (get var-2-nid-lu val))
                    (and (not (ru/is-select-var? key)) (nil? sel-val)) (assoc-in res [(get var-2-nid-lu key) :temporal-value] val)
                    :else res)))
          {} var-bindings))

(defn infinity-to-string
  "If any of the element is Infinity or -Infinity, stringify it"
  [bindings]
  (w/postwalk (fn [x]
                (cond (= x java.lang.Double/POSITIVE_INFINITY)
                      (str java.lang.Double/POSITIVE_INFINITY)

                      (= x java.lang.Double/NEGATIVE_INFINITY)
                      (str java.lang.Double/NEGATIVE_INFINITY)

                      :else x))
              bindings))

(defn string-to-infinity
  "If any of the element is \"Infinity\" or \"-Infinity\" then convert it to corresponding
  java.lang.double version"
  [bindings]
  (w/postwalk (fn [x]
                (cond (= x (str java.lang.Double/POSITIVE_INFINITY))
                      java.lang.Double/POSITIVE_INFINITY
                      (= x (str java.lang.Double/NEGATIVE_INFINITY))
                      java.lang.Double/NEGATIVE_INFINITY
                      :else x))
              bindings))

(defn solve [tpn bindings]
  (let [exprs-details (expr/make-expressions-from-map tpn)
        exprs (:all-exprs exprs-details)
        nid-to-var (:nid-2-var exprs-details)
        var-to-nid-lu (:var-2-nid exprs-details)
        solutions (solver/solve exprs nid-to-var 1 bindings)
        good-solution (first solutions)
        var-bindings (filter-temporal-choice-bindings (:bindings good-solution))
        new-bindings (var-to-node-bindings var-bindings var-to-nid-lu)]
    (println "solutions" (count solutions))
    (pprint (:metrics good-solution))
    {
     :tpn               tpn
     :previous-bindings bindings
     :bindings          new-bindings
     ; For my debug purpose
     ;:var-bindings var-bindings
     ;:expr-details exprs-details
     ;:solution     good-solution
     }))

;(pprint ["Bindings" (-> x :solution :bindings)
;         "Node bindings" (-> x :solution :node-bindings)
;         "node 2 var mapping" (-> x :expr-details :nid-2-var)
;         "var to nid mapping" (-> x :expr-details :var-2-nid)
;         ])
;["Bindings"
; {:v4-range [0 Infinity],
;  :v3-reward 0,
;  :v4-reward 0,
;  :v0-range 0,
;  :v3-range [0 Infinity],
;  :v3-cost 0,
;  :v1-range 0,
;  :v1-reward 0,
;  :v0-reward 0,
;  :v1-select true,
;  :v1-cost 0,
;  :v5-reward 0,
;  :v5-cost 0,
;  :v5-range [0 Infinity],
;  :v4-cost 0,
;  :v0-cost 0,
;  :v0-select :v1-select}
; "Node bindings"
; {:node-4 [[:v4-range [0 Infinity]] [:v4-reward 0] [:v4-cost 0]],
;  :node-10 [[:v3-reward 0] [:v3-range [0 Infinity]] [:v3-cost 0]],
;  :node-2
;  [[:v0-range 0] [:v0-reward 0] [:v0-cost 0] [:v0-select :v1-select]],
;  :node-12
;  [[:v1-range 0] [:v1-reward 0] [:v1-select true] [:v1-cost 0]],
;  :node-1 [[:v5-reward 0] [:v5-cost 0] [:v5-range [0 Infinity]]]}
; "node 2 var mapping"
; {:node-7 #{:v2 :v2-range :v2-select :v2-reward :v2-cost},
;  :node-4 #{:v4-range :v4-reward :v4-select :v4 :v4-cost},
;  :node-10 #{:v3-reward :v3-range :v3-cost :v3 :v3-select},
;  :node-1 #{:v5 :v5-reward :v5-select :v5-cost :v5-range},
;  :node-12 #{:v1 :v1-range :v1-reward :v1-select :v1-cost},
;  :node-2 #{:v0-range :v0 :v0-reward :v0-cost :v0-select}}
; "var to nid mapping"
; {:v2 :node-7,
;  :v2-range :node-7,
;  :v4-range :node-4,
;  :v3-reward :node-10,
;  :v4-reward :node-4,
;  :v5 :node-1,
;  :v1 :node-12,
;  :v0-range :node-2,
;  :v0 :node-2,
;  :v4-select :node-4,
;  :v3-range :node-10,
;  :v3-cost :node-10,
;  :v1-range :node-12,
;  :v1-reward :node-12,
;  :v0-reward :node-2,
;  :v2-select :node-7,
;  :v1-select :node-12,
;  :v1-cost :node-12,
;  :v4 :node-4,
;  :v3 :node-10,
;  :v5-reward :node-1,
;  :v2-reward :node-7,
;  :v5-select :node-1,
;  :v5-cost :node-1,
;  :v2-cost :node-7,
;  :v5-range :node-1,
;  :v4-cost :node-4,
;  :v0-cost :node-2,
;  :v0-select :node-2,
;  :v3-select :node-10}]
;=> nil

;---- find-bindings
;sample (:exprs :bindings :expr-values :var-code-length :satisfies :handled)
;initial bindings var
;{:v0-range 792887667569/500,
; :v2-range 792887667569/500,
; :v4-range 1585775366233/1000,
; :v5-range 1585775366233/1000,
; :v3-range 1585775366233/1000,
; :v0-select :v2-select,
; :v2-select true}