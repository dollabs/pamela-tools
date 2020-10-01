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
            [pamela.tools.utils.tpn-json :as pt-json]))

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

(defn- nid-2-var-range
  "Return node-id to range var mapping"
  [nid-2-var]
  (reduce (fn [result [nid vars]]
            (let [range-vars (filter (fn [var]
                                       (ru/is-range-var? var))
                                     vars)
                  range-var  (first range-vars)]

              (if-not (nil? range-var)
                (conj result {nid range-var})
                result)))
          {} nid-2-var))

;; Example
;:nid-2-var {:node-17 #{:v2 :v2-range :v2-select :v2-reward :v2-cost},
;            :node-1 #{:v3-reward :v3-range :v3-cost :v3 :v3-select},
;            :node-12 #{:v1 :v1-range :v1-reward :v1-select :v1-cost},
;            :node-7 #{:v0-range :v0 :v0-reward :v0-cost :v0-select}}}
(defn temporal-bindings-from-tpn-state
  "
  obj-times is time reached for each node in tpn
  Replaces node ids of obj-times with corresponding var ids
  nid-2-var is as produced by expression creator
  "
  [nid-2-var obj-times]
  (let [temporal-nid-2-var (nid-2-var-range nid-2-var)]
    (reduce (fn [result [uid time]]
              (conj result {(uid temporal-nid-2-var) time}))
            {} obj-times)))

(defn get-successful-solutions [solutions]
  (remove nil? (map (fn [sol]
                      (if (= 0 (get-in sol [:metrics :fail-count]))
                        sol))
                    solutions)))

(defn solve-internal [tpn exprs nid-to-var var-to-nid-lu n-iterations expression-bindings]
  (let [n-iterations   (or n-iterations 1)
        solutions      (solver/solve exprs nid-to-var n-iterations expression-bindings)
        good-solutions (get-successful-solutions solutions)
        good-solution  (first good-solutions)
        new-bindings   (when good-solution
                         (let [var-bindings (filter-temporal-choice-bindings (:bindings good-solution))]
                           (var-to-node-bindings var-bindings var-to-nid-lu)))]

    (println "Total solutions" (count solutions))
    (println "Good solutions" (or (count good-solutions) 0))
    (when good-solution (pprint (:metrics good-solution)))
    {
     :tpn                          tpn
     :previous-expression-bindings expression-bindings
     :bindings                     new-bindings
     :nid-2-var                    nid-to-var}))
; For my debug purpose
;:var-bindings var-bindings
;:solution     good-solution



(defn solve [tpn & [bindings n-iterations]]
  "bindings are assumed to be nil or expression variables and their values.
  Use temporal-bindings-from-tpn-state to convert from node bindings to expression bindings"
  (let [exprs-details (expr/make-expressions-from-map tpn)
        exprs         (:all-exprs exprs-details)
        nid-to-var    (:nid-2-var exprs-details)
        var-to-nid-lu (:var-2-nid exprs-details)]
    (solve-internal tpn exprs nid-to-var var-to-nid-lu n-iterations bindings)))

(defn solve-with-node-bindings [tpn node-bindings n-iterations]
  (let [exprs-details (expr/make-expressions-from-map tpn)
        exprs         (:all-exprs exprs-details)
        nid-to-var    (:nid-2-var exprs-details)
        var-to-nid-lu (:var-2-nid exprs-details)]
    (merge (solve-internal tpn exprs nid-to-var var-to-nid-lu n-iterations (temporal-bindings-from-tpn-state nid-to-var node-bindings))
           {:previous-bindings node-bindings
            :expr-details      exprs-details})))

(defn solve-with-node-bindings-file [tpn-file bindings-file n-iterations]
  (let [{tpn :tpn bindings :bindings} (pt-json/read-tpn-with-bindings tpn-file bindings-file)]
    (solve-with-node-bindings tpn bindings n-iterations)))

(defn solve-and-write-bindings
  "Read and solve TPN from given file
   Write bindings to given file"
  [tpn-file to-bindings-file n-iterations]
  (let [result   (solve-with-node-bindings-file tpn-file nil n-iterations)
        bindings (if (:bindings result)
                   {:bindings (:bindings result)}
                   (do (println "No bindings found for tpn:" tpn-file)
                       {:bindings {}}))]
    (pt-json/write-bindings-to-json bindings to-bindings-file)
    bindings))

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