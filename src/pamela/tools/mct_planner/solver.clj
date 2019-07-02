;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.solver
  (:require [pamela.tools.mct-planner.sample-i :refer :all]
            [pamela.tools.mct-planner.expr :as expr]
            [pamela.tools.mct-planner.util :refer :all]
            [pamela.tools.mct-planner.coding-length :refer :all]
            [pamela.tools.mct-planner.expr-check :as ex-check]
            [pamela.tools.mct-planner.learning :as mlearn]  ;parameter learning
            [pamela.tools.mct-planner.weighted-probability]
            [pamela.tools.mct-planner.solver-support :as ss]
            [clojure.pprint :refer :all]
            [clojure.core.matrix :as mat]
            [clojure.data :as data]
            [clojure.set :as set]
            [pamela.tools.utils.util :as util]))

(def debug false)
(def with-static-uncertainty false)
(def with-learning true)
;; Notes from 5/19/2016
; Compute code lengths -log2 P where P is probability of event.
; For uncontrollable activity, P is 1. For range, P is ub - lb + 1
; Need to compute code-length for each expression.
; Choose expr with shortest code-length
; assign code length from bound vars
; Need to pick value for all expressions.

; 1. Pick exprs that are enabled. i.e fromvars are bound, expr does not has a assigned value.
; 2. Compute code length for each expression. Sort by increasing length
; 3. propagate var for the top expr. Also assign the propagated value as the value of the expression.
; 4. Check exprs. i.e expr value is within constraints. For uncontrollable, the assigned value will always satisfy because the
; constraint itself was used to propagate the value. For controllable, ?? TODO

; Note: Try to avoid nil values for var bindings and expr values.

(defn set-choice-algo [static-unc? learning?]
  (def with-static-uncertainty static-unc?)
  (def with-learning learning?)
  (ss/set-choice-algo static-unc? learning?)
  #_(println "solver - choice algo with-static-uncertainty" with-static-uncertainty "with-learning" with-learning))

(defn expr-has-value? [expr values-m]
  (if-not (nil? (get values-m expr))
    true false))

(defn exprs-enabled [solution nid-2-var]
  (filter (fn [expr]
            (cond (= (:symbol expr) 'selector=)             ;we assume from vars are always bound
                  (do
                    ;(println "expr" expr)
                    #_(println "expr value" (expr-has-value? expr (:expr-values solution)) "or choice value"
                               (pamela.tools.mct-planner.util/selector=chosen expr (:bindings solution) nid-2-var))
                    ;(pprint nid-2-var)
                    (cond (expr-has-value? expr (:expr-values solution))
                          false
                          (> (count (pamela.tools.mct-planner.util/selector=chosen expr (:bindings solution) nid-2-var))
                             0)
                          false
                          :else
                          true))
                  (= (:symbol expr) 'if=)
                  (and (if=chosen expr (:bindings solution))
                       (expr/from-vars-bound? expr (:bindings solution))
                       ; We need expression values to keep track of chosen path for choice options.
                       (not (expr-has-value? expr (:expr-values solution))))
                  :else
                  (if (and (not (expr-has-value? expr (:expr-values solution)))
                           (expr/from-vars-bound? expr (:bindings solution))
                           )
                    true false)
                  ))
          (:exprs solution)))

(defn make-sample
  "Return an instance of sample record"
  [exprs & [initial-bindings]]
  (let [bindings (if (and initial-bindings (pos? (count initial-bindings)))
                   initial-bindings
                   (expr/get-initial-bindings exprs))

        expr-values {}
        ; Initial coding length of begin vars is 0
        var-code-lengths (reduce (fn [result var]
                                   (merge result {var 0}))
                                 {} (keys bindings))
        handled #{}                                         ;set of expressions that have been handled
        ]
    (conj (pamela.tools.mct-planner.sample-i/->sample exprs bindings expr-values var-code-lengths {}) {:handled handled})))

(defn compute-code-length [enabled var-code-lengths]
  "enabled is a list of expressions. code-lengths is a mapping of vars and a coding length value.
   A var that does not has a value is assumed to have 0 code length."
  (let [clenths (map (fn [expr]
                       [expr (expr-code-length expr var-code-lengths)])
                     enabled)
        sorted (sort-by second < clenths)]
    sorted))

(defmulti bind-var
          "For the given expr, assign a value for the :to-var
          Updates sample with bindings, the expr value used to bind to-var,
          and code length for the to-var.
          returns updated sample."
          (fn [expr code-length _]
            ;(println "bind-var:" (:to-var expr) "\nexpr-code-length:" code-length
            ;         "\nexpr:" (:symbol expr) "\n")
            ;(println "impl-bind-var")
            ;(pprint expr code-length)
            (:symbol expr)))

(defmethod bind-var :default [expr _ _]
  (util/to-std-err (println "Impl bind-var" (:symbol expr) expr))
  ; returning nil here so that find-sample loop terminates
  nil)

(defn update-sample [expr to-var-value expr-value to-var-code-length solution]
  (if (not (nil? (get-in solution [:expr-values expr])))
    (util/to-std-err (println "Warning: debug me: Expr" expr "has a value:" (get-in solution [:expr-values expr])
                              "new value:" expr-value)))

  ; var-code-length holds only the most recent value and will be updated over time for certain vars.
  #_(if (not (nil? (get-in solution [:var-code-length (:to-var expr)])))
      (util/to-std-err (println "Warning: Expr" expr "var-code-length has a value:" (get-in solution [:var-code-length (:to-var expr)])
                                "new value:" to-var-code-length)))
  ;(println "update-sample\n" "expr:" expr "\nvar-val expr-val" to-var-value expr-value)
  (let [to-var-bound (and (expr/to-var-bound? expr (:bindings solution))
                          (not= to-var-value (get-in solution [:bindings (:to-var expr)])))]
    (when (and to-var-bound debug) (util/to-std-err (do
                                                      ;Fixme -- update-sample ; Reproducible via pamela.tools.mct-planner.solver-test/gen-fail-binding-all for simple-choice.tpn.json
                                                      ;Warning: to-var is bound: bound-value [3 13] new value: [3 18]
                                                      ;expr value: [3 18] #pamela.tools.mct-planner.expr.expr{:symbol =, :to-var :v23-range, :from-vars #{:v16-range}, :value nil, :controllable false, :m {:object :na-33, :from-nodes #{:node-28}}}
                                                      (println "Fix me -- update-sample")
                                                      (println "Warning: to-var is bound: bound-value" (get-in solution [:bindings (:to-var expr)])
                                                               "new value:" to-var-value)
                                                      (println "expr value:" expr-value expr))))
    (if to-var-bound
      (merge solution {:expr-values     (conj (:expr-values solution) {expr (get-in solution [:bindings (:to-var expr)])})
                       :var-code-length (conj (:var-code-length solution) {(:to-var expr) to-var-code-length})
                       })
      (merge solution {:bindings        (conj (:bindings solution) {(:to-var expr) to-var-value})
                       :expr-values     (conj (:expr-values solution) {expr expr-value})
                       :var-code-length (conj (:var-code-length solution) {(:to-var expr) to-var-code-length})
                       }))))

(defn bind-controllable-bounds [expr]
  (if with-learning
    (mlearn/sample-from-monte (get-in expr [:m :monte-learner]))
    (random-bound (:value expr))))

(defmethod bind-var 'in-range [expr expr-code-length solution]
  ;(println "bind-var 'in-range has monte-learner" (contains?  (:m expr) :monte-learner))
  (if-not (var-bound? (:to-var expr) (:bindings solution))
    (let [bindings (:bindings solution)
          val (if (:controllable expr)
                (bind-controllable-bounds expr)
                (:value expr))
          to-val (mat/add val (get bindings (first (:from-vars expr))))]
      (update-sample expr to-val val expr-code-length solution))
    (let [bindings (:bindings solution)
          to-val ((:to-var expr) bindings)
          from-val ((first (:from-vars expr)) bindings)
          expr-value (mat/sub to-val from-val)
          ]
      ;(println "to-var is bound")
      #_(util/to-std-err (println "?? bind-var 'in-range to-var is bound " (:to-var expr) "="
                                  ((:to-var expr) (:bindings solution)))
                         )
      (update-sample expr to-val expr-value expr-code-length solution))))

(defmethod bind-var 'range<= [expr expr-code-length solution]
  (if-not (var-bound? (:to-var expr) (:bindings solution))
    (let [val (:value expr)
          to-val (mat/add val (get (:bindings solution) (first (:from-vars expr))))]
      (update-sample expr to-val val expr-code-length solution))
    (let [bindings (:bindings solution)
          to-val ((:to-var expr) bindings)
          from-val ((first (:from-vars expr)) bindings)
          expr-value (mat/sub to-val from-val)
          ]
      ;(println "to-var is bound")
      #_(util/to-std-err (println "?? bind-var 'in-range to-var is bound " (:to-var expr) "="
                                  ((:to-var expr) (:bindings solution)))
                         )
      (update-sample expr to-val expr-value expr-code-length solution))
    ))

; TODO We should always check if to-var is bound or not
; if bound, update expression value
; if not-bound, update binding and expression value
(defmethod bind-var 'cost-max [expr code-length solution]
  (let [to-val (apply max (expr/from-values expr (:bindings solution)))]
    (update-sample expr to-val to-val code-length solution)))

(defmethod bind-var 'reward-max [expr code-length solution]
  (let [to-val (apply max (expr/from-values expr (:bindings solution)))]
    (update-sample expr to-val to-val code-length solution)))

(defn display-diff [info diff]
  (println info)
  (when (first diff)
    (println "only in first")
    (pprint (first diff)))
  (when (second diff)
    (println "only in second")
    (pprint (second diff))))

(defmethod bind-var 'in-range-max [expr code-length solution]
  (let [bounds (expr/from-values expr (:bindings solution))
        to-val (get-in solution [:bindings (:to-var expr)])
        to-val (if-not (nil? to-val)
                 to-val
                 (pamela.tools.mct-planner.util/get-max-max-bounds bounds))
        updated-smpl (update-sample expr to-val to-val code-length solution)]
    ;(println "bind-var 'in-range-max" )
    ;(println "bounds" bounds)
    ;(println "to-val" to-val)
    ;(println "Updated sample")
    ;(pprint (:bindings updated-smpl))
    ;(println "range-max bindings diffs")
    ;(display-diff "Bindings" (data/diff (:bindings solution) (:bindings updated-smpl)))
    ;(println)
    ;(display-diff "expr-values" (data/diff (:expr-values solution) (:expr-values updated-smpl)))
    ;(println)
    ;(display-diff "var-code-length" (data/diff (:var-code-length solution) (:var-code-length updated-smpl)))
    ;(println)
    updated-smpl))

(defmethod bind-var '= [expr code-length solution]
  (let [to-val ((first (:from-vars expr)) (:bindings solution))]
    (update-sample expr to-val to-val code-length solution)))

(defmethod bind-var 'cost= [expr code-length solution]
  (let [to-val (+ (:value expr) ((first (:from-vars expr)) (:bindings solution)))]
    (update-sample expr to-val to-val code-length solution)))

(defmethod bind-var 'reward= [expr code-length solution]
  (let [to-val (+ (:value expr) ((first (:from-vars expr)) (:bindings solution)))]
    (update-sample expr to-val to-val code-length solution)))

(defn select-with-static-uncertainty [expr]
  (pamela.tools.mct-planner.weighted-probability/sample-weighted-distribution (get-in expr [:m :weighted-distribution]))
  )

(defmethod bind-var 'selector= [expr code-length solution]
  (let [to-val (cond (true? with-static-uncertainty)
                     (select-with-static-uncertainty expr)
                     :else
                     (rand-nth (into [] (:from-vars expr))))]
    (update-sample expr to-val to-val code-length solution)))

(defmethod bind-var 'if= [expr code-length solution]
  ; :value of the expression is a choice edge, [from to]
  ; if the from-value is == to, then the path is considered to be chosen.
  (let [from-value (get (:bindings solution) (first (:value expr)))
        path-chosen? (= from-value (second (:value expr)))
        to-val (get (:bindings solution) (first (:from-vars expr)))]
    (if path-chosen?
      (update-sample expr to-val path-chosen? code-length solution)
      (merge solution {:expr-values (conj (:expr-values solution) {expr path-chosen?})}))))

;
; enabled --> An expr is enabled if it does not has a value.
; an expr is assigned a value after it's all vars are bound.
; an expr value should be same? as to-var value?
; If an expr has from-vars bound but not to-var, then the to-var is bound

; the function below implements following algo
; 1. find enabled expressions
; order enabled expr by code-length
; bind to-var if necessary
; assign value to expr
; go-to 1 until no more enabled expr

(defn find-bindings-old
  "Finds and updates bindings for the sample"
  [exprs nid-2-var & [initial-bindings learnt-params]]
  ;(println "find-bindings exprs count" (count exprs))
  ;(pprint exprs)
  (let [var2nid (pamela.tools.mct-planner.util/var-2-nid nid-2-var)]
    (loop [sample (make-sample exprs initial-bindings learnt-params)]
      ;(println "\n---- find-bindings")
      ;(println "sample")
      ;(pprint sample)
      ;(println "initial bindings var")
      ;(pprint (:bindings sample))
      ;(println "initial bindings node")
      ;(pprint (pamela.tools.mct-planner.util/var-to-node-bindings (:bindings sample) nid-2-var))
      (let [enabled (exprs-enabled sample nid-2-var)        ;Expressions that do not have a value.
            ;_ (println "enabled")
            ;_ (pprint enabled)
            ;_ (do (println "var-code-length")
            ;      (pprint (:var-code-length sample)))
            expr-code-lengths (compute-code-length enabled (:var-code-length sample))
            expr-code-length (first expr-code-lengths)
            the-expr (first expr-code-length)
            _ (do (println "\nenabled ordered by expr-code-lengths")
                  (pprint expr-code-lengths))
            _ (do (println "\nbinding var" (:to-var the-expr))
                  (println "expr:")
                  (pprint the-expr)
                  #_(println "expr-code-length:" (second expr-code-length))
                  )
            updated-sample (if (pos? (count expr-code-length))
                             (bind-var the-expr (second expr-code-length) sample)
                             sample)
            #_(do (when the-expr
                    (println "Bound value:" (:to-var the-expr) "(" ((:to-var the-expr) var2nid) ")" (get-in updated-sample [:bindings (:to-var the-expr)]))
                    (println "the-expr value" (get-in updated-sample [:expr-values the-expr]))
                    ))

            ; TODO We only bind value for 1 expression at a time, so the code below can be simplified.
            prev-bound-expr (into #{} (keys (:expr-values sample)))
            updated-bound-expr (into #{} (keys (:expr-values updated-sample)))
            new-bound-expr (select-keys (:expr-values updated-sample) (set/difference updated-bound-expr prev-bound-expr))
            sat-status (into {} (map (fn [expr]
                                       {(first expr)
                                        (ex-check/bindings-satisfy-expr (first expr) (:bindings updated-sample))}) new-bound-expr))
            all-expr-sat (merge (:satisfies updated-sample) sat-status)
            updated-sample (merge updated-sample {:satisfies all-expr-sat})]

        (if (zero? (count enabled)) #_(nil? (next expr-code-lengths))
          (do
            ;(println "Finished find-bindings")
            ;(println "Count of :satisfies and :expr-values" (count (:satisfies updated-sample))
            ;         (count (:expr-values updated-sample)))
            ;(println "var bindings" (:bindings sample))
            ;(println "node bindings" (pamela.tools.mct-planner.util/var-to-node-bindings (:bindings updated-sample) nid-2-var))
            (merge updated-sample
                   {:node-bindings (pamela.tools.mct-planner.util/var-to-node-bindings (:bindings updated-sample) nid-2-var)}))
          (recur updated-sample))))))

#_(defn update-monte-in-range [m-learner expr-value sats-expr?]
    (let [vals (pamela.tools.mct-planner.util/to-bounds expr-value)
          vals (into #{} (range (first vals) (+ 1 (second vals))))
          common-vals (set/intersection vals (into #{} (:values m-learner)))]
      (doseq [v common-vals]
        (mlearn/update-monte m-learner v sats-expr?))))

#_(defn update-monte-learner-1 [sample]                     ; saved on 11/20
    ; learn only for expressions that have values
    (let [all-succ (every? (fn [sats] sats) (vals (:satisfies sample)))]
      ;(println "All expr passed" all-succ "expr-values count" (count (:expr-values sample)))
      ;(pprint (:expr-values sample))
      (doseq [[expr exp-value] (:expr-values sample)]
        ;(println "expr value" exp-value expr)
        (if (contains? (get expr :m) :monte-learner)
          (let [m-learner (get-in expr [:m :monte-learner])
                ;sats-expr (get (:satisfies sample) expr)
                expr-value (:value expr)]
            (cond (= 'in-range (:symbol expr))
                  (do
                    ;(println expr)
                    ;(pprint expr)
                    ;(println "update monte learner expr value" exp-value "all-succ" all-succ)
                    ;(println m-learner)
                    (mlearn/update-monte m-learner exp-value all-succ (first expr-value) (second expr-value) :time))
                  (or (= 'cost= (:symbol expr)) (= 'max-cost (:symbol expr)))
                  (do
                    ;(println expr)
                    ;(pprint expr)
                    ;(println "update monte learner expr value" exp-value "sats" all-succ)
                    ;(println m-learner)
                    (mlearn/update-monte m-learner exp-value all-succ 0 expr-value :cost))
                  (or (= 'reward= (:symbol expr)) (= 'min-reward (:symbol expr)))
                  (do
                    ;(println expr)
                    ;(pprint expr)
                    ;(println "update monte learner expr value" exp-value "sats" all-succ)
                    ;(println m-learner)
                    (mlearn/update-monte m-learner exp-value all-succ 0 expr-value :reward))
                  (= 'selector= (:symbol expr))
                  (do
                    ;(println "choice:" expr)
                    ;(println "update monte learner expr value" exp-value "sats" all-succ)
                    ;(println m-learner)
                    (mlearn/update-monte m-learner exp-value all-succ)
                    )

                  :else
                  (util/to-std-err (println "Handle update-monte-learner for expr type" (:symbol expr))
                                   (pprint expr)))
            ;(println "Updating monte learner" expr)
            ;(println (get-in expr [:m :monte-learner]))
            ;(println (keys @(:storage m-learner)))
            ;(println (keys m-learner))
            ;(println "config-options" (:config-options m-learner))
            )
          (util/to-std-err (println "Expr has a value but no monte learner" (:value expr) expr exp-value) ;should only happend for [0 Infinity] case
                           #_(throw (Exception. (str "Expr has a value but no monte learner: " expr ", " exp-value))))
          ))))

(defn organize-bindings
  "Given a bindings map, organize vars and their values in following classes
  :in-range, :cost, :reward, :others"
  [bindings]
  (reduce (fn [result [var val]]
            (cond (is-range-var? var)
                  (if (vector? val)
                    (assoc-in result [:time var] (second val)) ;upper bound
                    (assoc-in result [:time var] val)
                    )

                  (is-cost-var? var)
                  (assoc-in result [:cost var] val)
                  (is-reward-var? var)
                  (assoc-in result [:reward var] val)
                  :else
                  (assoc-in result [:others var] val)))
          {} bindings))

(defn get-totals [bindings]
  (let [org-bindings (organize-bindings bindings)
        time-vals (vals (:time org-bindings))
        min-time (apply min time-vals)
        max-time (apply max time-vals)
        total-time (- max-time min-time)
        costs (vals (:cost org-bindings))
        total-cost (if costs (apply max costs) 0)
        rewards (vals (:reward org-bindings))
        total-reward (if rewards (apply max rewards) 0)]
    {:total-time   total-time
     :total-cost   total-cost
     :total-reward total-reward}))

(defmulti update-learning-for-expr
          (fn [expr for-expr-value all-succ]
            (:symbol expr)))

(defmethod update-learning-for-expr :default [expr for-expr-value all-succ]
  (util/to-std-err
    (println "Warning: Impl: update-learning-for-expr" (:symbol expr) for-expr-value all-succ)
    (pprint expr))
  (throw (Exception. (str "Handle expr-enabled? " (:symbol expr)))))

(defmethod update-learning-for-expr 'selector= [expr for-expr-value all-succ]
  (mlearn/update-monte (get-in expr [:m :monte-learner]) for-expr-value all-succ))

(defmethod update-learning-for-expr 'in-range [expr for-expr-value all-succ]
  (mlearn/update-monte (get-in expr [:m :monte-learner]) for-expr-value all-succ (first (:value expr)) (second (:value expr)) :time))

(defmethod update-learning-for-expr 'max-cost [expr for-expr-value all-succ]
  (let [m-learn (get-in expr [:m :monte-learner])
        mn (apply min (:values m-learn))
        mx (apply max (:values m-learn))]
    (when all-succ
      #_(println "Learning for max-cost value " for-expr-value [mn mx]))
    (mlearn/update-monte (get-in expr [:m :monte-learner]) for-expr-value all-succ mn mx :cost)))

(defmethod update-learning-for-expr 'min-reward [expr for-expr-value all-succ]
  (let [m-learn (get-in expr [:m :monte-learner])
        mn (apply min (:values m-learn))
        mx (apply max (:values m-learn))]
    (when all-succ
      #_(println "Learning for min-reward value " for-expr-value [mn mx]))
    (mlearn/update-monte (get-in expr [:m :monte-learner]) for-expr-value all-succ mn mx :reward)))

(defn update-monte-learner [sample]
  ; learn only for expressions that have values
  ; learning based on realized time, cost, and reward
  (let [all-succ (every? (fn [sats] sats) (vals (:satisfies sample)))
        ;max-tcr (get-totals (:bindings sample))
        ]
    ;(println "All expr passed?\"" all-succ "\"expr-values count" (count (:expr-values sample)))
    ;(pprint (:expr-values sample))
    (doseq [[expr exp-value] (:expr-values sample)]
      ;(println "expr value" exp-value expr)
      (if (contains? (get expr :m) :monte-learner)
        (update-learning-for-expr expr exp-value all-succ)
        (util/to-std-err (println "Expr has a value but no monte learner" (:value expr) expr exp-value) ;should only happend for [0 Infinity] case
                         #_(throw (Exception. (str "Expr has a value but no monte learner: " expr ", " exp-value))))
        ))))

#_(defn update-monte-learner [expr expr-value sats-expr?]
    (if (contains? (get expr :m) :monte-learner)
      (let [m-learner (get-in expr [:m :monte-learner])]
        (cond (= 'in-range (:symbol expr))
              (do
                ;(println expr)
                ;(pprint expr)
                (println "update monte learner expr value" expr-value)
                ;(println m-learner)
                (update-monte-in-range m-learner expr-value sats-expr?))

              :else
              (util/to-std-err (println "Handle update-monte-learner for expr type" (:symbol expr))
                               (pprint expr)))
        ;(println "Updating monte learner" expr)
        ;(println (get-in expr [:m :monte-learner]))
        ;(println (keys @(:storage m-learner)))
        ;(println (keys m-learner))
        ;(println "config-options" (:config-options m-learner))
        )))

(defn print-expr-bindings [expr bindings]
  (let [from-vars (:from-vars expr)
        from-vals (into [] (select-keys bindings from-vars))
        to-var (:to-var expr)
        to-val (get bindings to-var)
        ]
    (println (:symbol expr) [to-var to-val] " is assigned from " from-vals "\n")
    ))

(defn find-bindings [exprs nid-2-var & [initial-bindings]]
  (let [var2nid (pamela.tools.mct-planner.util/var-2-nid nid-2-var)]
    (loop [sample (make-sample exprs initial-bindings)
           ]
      ;(println "\n---- find-bindings")
      ;(println "sample" (keys sample))
      ;(pprint sample)
      ;(println "initial bindings var")
      ;(pprint (:bindings sample))
      ;(println "initial bindings node")
      ;(pprint (pamela.tools.mct-planner.util/var-to-node-bindings (:bindings sample) nid-2-var))
      (let [enabled (ss/enabled-exprs (:exprs sample) (:bindings sample) (:handled sample))
            ;_ (println "enabled")
            ;_ (pprint enabled)
            ;_ (do (println "var-code-length")
            ;      (pprint (:var-code-length sample)))
            expr-code-lengths (compute-code-length enabled (:var-code-length sample))
            expr-code-length (first expr-code-lengths)
            the-expr (first expr-code-length)
            #_(do (println "\nenabled ordered by expr-code-lengths")
                  (pprint expr-code-lengths))
            {bindings    :bindings
             expr-values :expr-values} (if (pos? (count expr-code-length))
                                         (do
                                           ;(println "\npropagate expr:")
                                           ;(pprint the-expr)
                                           (let [return-val (ss/propagate the-expr (:bindings sample) (:expr-values sample))
                                                 new-bindings (:bindings return-val)
                                                 from-var (-> the-expr :from-vars first)
                                                 to-var (-> the-expr :to-var)
                                                 from-val (get new-bindings from-var)
                                                 to-val (get new-bindings to-var)
                                                 ]
                                             #_(println "propagate " (:symbol the-expr) to-var "(" to-val ") is assigned from " from-var "(" from-val ")\n")
                                             #_(println "propagate " (:symbol the-expr) (to-var var2nid) "(" to-val ") is assigned from " (from-var var2nid) "(" from-val ")\n")
                                             return-val))
                                         {:bindings    (:bindings sample)
                                          :expr-values (:expr-values sample)
                                          })
            #_(do (println "expr" the-expr "value" expr-value)
                  (pprint bindings))
            expr-sats (if the-expr
                        (ss/expr-satisfies-contraint? the-expr bindings))
            satisfies (if the-expr
                        (ss/update-expression-value the-expr expr-sats (:satisfies sample)))
            #_(when (and the-expr (not expr-sats))
                (println "Failed expr")
                (pprint the-expr)
                (print-expr-bindings the-expr bindings)
                (println "bindings")
                (pprint bindings))
            handled (if the-expr
                      (conj (:handled sample) the-expr)
                      (:handled sample))]

        ;(println "expr sats" the-expr "\n" expr-sats "handled")
        ;(pprint handled)
        ;(println "expr-values")
        ;(pprint expr-values)
        (if (zero? (count enabled)) #_(nil? (next expr-code-lengths))
          (do
            (when with-learning
              (update-monte-learner sample))
            (merge sample {:node-bindings (pamela.tools.mct-planner.util/var-to-node-bindings bindings nid-2-var)})
            ;(println "Finished find-bindings")
            ;(println "Count of :satisfies and :expr-values" (count (:satisfies updated-sample))
            ;         (count (:expr-values updated-sample)))
            ;(println "var bindings" (:bindings sample))
            ;(println "node bindings" (pamela.tools.mct-planner.util/var-to-node-bindings (:bindings updated-sample) nid-2-var))
            )
          (recur (merge sample {:bindings    bindings
                                :expr-values expr-values
                                :handled     handled
                                :satisfies   satisfies
                                }))))
      )))


(defn get-failed-exprs
  "Given n samples for n interations, return count of expressions failed"
  [samples]
  (let [flattened (reduce (fn [result sample]
                            (into result (:satisfies sample))) [] samples)]
    (reduce (fn [result sample]
              (if (second sample)
                result
                (update result (first sample) (fn [old-value]
                                                (if (nil? old-value)
                                                  1
                                                  (inc old-value)))))
              ) {} flattened)))

(defn get-failed-exprs-for-a-sample
  "Given a sample, return a set of expressions for which bindings do not satisfy the expression"
  [sample]
  (into #{} (remove nil? (map (fn [[expr sat?]]
                                (if-not sat?
                                  expr)) (:satisfies sample)))))

(defn get-satisfy-bindings-exprs-for-a-sample
  "Given a sample, return a set of expressions for which bindings do satisfy the expression"
  [sample]
  (into #{} (remove nil? (map (fn [[expr sat?]]
                                (if sat?
                                  expr)) (:satisfies sample)))))

(defn make-sample-metrics
  "For the given sample, add mission-time = max(bindings) - min(bindings)
  failure-count, total-cost, total-reward and suggested-changes for failed constraints
  and return updated sample"
  [sample]
  ;(println "sample")
  ;(pprint sample)
  (let [{bindings :bindings} sample
        {total-time   :total-time
         total-cost   :total-cost
         total-reward :total-reward} (get-totals bindings)
        failed-exprs (get-failed-exprs-for-a-sample sample)
        failed-exprs (reduce (fn [result expr]
                               (conj result (merge expr {:bound-value (pamela.tools.mct-planner.expr-check/normalize-expr-value expr bindings)}))
                               )
                             [] failed-exprs)]
    (when debug
      (println "Failed exprs" (count failed-exprs))
      (doseq [expr failed-exprs]
        (println (get bindings (:to-var expr)) "=" expr)))

    (conj sample {:metrics {:total-time            total-time
                            :total-cost            total-cost
                            :total-reward          total-reward
                            :failed-exprs          failed-exprs
                            :fail-count            (count failed-exprs)
                            :number-of-expressions (count (:exprs sample))
                            ;:bindings bindings
                            }})))

(defn collect-metrics
  [samples]
  (reduce (fn [res sample]
            (conj res (:metrics sample)))
          [] samples))

(defn default-sample-sort-comp [x y]
  ;(println "comparing" x y)
  (compare [(:fail-count x) (:total-time x) (:total-cost x) (:total-reward y)]
           [(:fail-count y) (:total-time y) (:total-cost y) (:total-reward x)]))

(defmacro make-sample-sort-comp
  "a-vec specifies sort order for keys and comparator as:
   [[:fail-count :increasing] [:total-time :increasing] [:total-cost :increasing] [:total-reward :decreasing]]
   This example sorts first by low failures, then by shortest time, then by lowest cost and then by maximum reward.
   "
  [a-vec]
  ;(println "Sort spec no quote" a-vec)
  (let [local-x (gensym "local-x-")
        local-y (gensym "local-y-")
        ;_ (println local-x local-y)
        ab (reduce (fn [result [akey sort-order]]
                     ;(println "reduce" akey sort-order)
                     ;(pprint result)
                     (if (= sort-order :increasing)
                       {:a (conj (:a result) `(~akey ~local-x))
                        :b (conj (:b result) `(~akey ~local-y))}
                       {:a (conj (:a result) `(~akey ~local-y))
                        :b (conj (:b result) `(~akey ~local-x))}
                       ))
                   {:a [] :b []} a-vec)]
    ;(println "final ab")
    ;(pprint ab)
    `(fn [x# y#]
       (let [~local-x x#
             ~local-y y#]
         ;(println `x# "is" x#  "local x var is" ~local-x)
         ;(println "comparing" ~local-x ~local-y)
         (compare ~(:a ab) ~(:b ab))
         ))))

(def macro-gen-default-sort-comp (make-sample-sort-comp [[:fail-count :increasing] [:total-time :increasing]
                                                         [:total-cost :increasing] [:total-reward :decreasing]
                                                         ]))


#_(defn do-learning [sample]
    #_(println "do-learning")
    #_(pprint sample)
    #_(doseq [expr (:exprs sample)]
        (when (and (= 'in-range (:symbol expr))
                   (expr/controllable? expr))
          (println "learning controllable expr")
          (println expr)
          (println "Satisfies" (get-in sample [:satisfies expr]))
          (println "Expr value" (get-in sample [:expr-values expr]))
          (println "Binding" (get-in sample [:bindings (:to-var expr)]))
          ))
    #_(println-monte-learner-stats (:exprs sample))
    sample)

; A convenient place to keep track of all expression types. A helpful debugging aid
(defonce expr-set (atom #{}))
(defn update-exprs-set [all-exprs]
  (doseq [expr all-exprs]
    (swap! expr-set conj (:symbol expr))))

(defonce debug-state (atom {}))
(defn collect-state [exprs nid2var initial-bindings]
  (swap! debug-state conj {:exprs exprs :nid2var nid2var :initial-bindings initial-bindings}))

(defn solve
  "For the given expressions, perform n-iterations and
   return count of successful bindings for each expression."
  [exprs nid-2-var iterations & [initial-bindings]]
  (when debug (update-exprs-set exprs))

  (let [samples (reduce (fn [result _]
                          (when debug (collect-state exprs nid-2-var initial-bindings))
                          (conj result (find-bindings exprs nid-2-var initial-bindings)))
                        [] (range iterations))]
    #_(println "Got samples" (count samples))

    (reduce (fn [result sample]
              (conj result (make-sample-metrics sample))) [] samples)))

(defn solve-expressions [exprs-from-file iterations]
  (solve (:all-exprs exprs-from-file) (:nid-2-var exprs-from-file) iterations))

(defn solve-for-tpn [json-file & [iterations]]
  (let [{exprs :all-exprs nid-2-var :nid-2-var :as info} (expr/make-expressions-from-file json-file)]
    ;(pprint info)
    ;(println "keys" (keys info))
    (solve exprs nid-2-var (or iterations 1))
    nil))

#_(defn debug-solver [exprs & [initial-bindings]]
    (println "Given bindings" initial-bindings)
    (pprint initial-bindings)
    (let [sample (first (solve exprs 1 initial-bindings))]
      (println "\nnew-bindings")
      (pprint (:bindings sample))
      (println "expr values")
      (pprint (:expr-values sample))
      ))

(defn count-failures-for-file [file-name]
  (let [expr-info (expr/make-expressions-from-file file-name)
        samples (solve (:exprs expr-info) (:nid-2-var expr-info) 1)
        failed-expressions (get-failed-exprs samples)]
    (println "Count of expressions failed" (count failed-expressions))
    ;(pprint failed-expressions)
    ;(println failed-expressions)
    ;(pprint (first samples))
    failed-expressions))

; Solver notes 6/15/2018
; Given a set of expressions, find enabled expressions.
;  * An expression is enabled if it has from bindings and does not has a value.
;   * If the end vars of the expression are bound, then we compute the value of the expression using
;   * to vars and from var
;   * if the to vars are not bound, then we use constraint to compute the expression value and bind the to vars.
;  * Now that we have a value for the expression, we test if the bindings satisfy the constraint.
; Uncertainty in some expressions.
;  * Some expressions have uncertainty in them. Ex: in-range constraints. We compute code length for them
;    * Other expressions such as = don't have uncertainty, so code length for them is 0.
;    * Given a set of enabled expressions, we choose the expression with lowest code length
; Until today(June 15th, 2018) we used uniform probability distribution to compute code length
; Now we want to use a distribution function that favors success of all expressions but occasionally
; chooses values that have caused failures or unseen values with some probability distribution.
; For each expression value, we will keep track of success and failures and total iterations
;  * This will give us the distribution function over know and unknown values. We will use this function to choose
; next value for the expression.

; Experiment notes, 6/19/2018
; exp1. Run experiments to find when do we find the first successfull sample. Find mean and variance
; exp2. Run experiments with N iterations. find number of succesful samples. Find mean and variance
; Perform exp1 and exp2 with
;  - a choice path being selection with equal probability
;  - a choice path being selected based on uncertainty / code-length
;  - a choice path being selected based on learning. i.e A successful choice path has a higher probability of chosen next time versus the rest.

;; Chat with Paul 8/18/2018
; Remember in between constraints. If the to-var is bound, then choose expression value
; to be constraint by the to-var.
; add <= constraint for p-end nodes
; = constraint is bi directional. If one var has a value, then the other var has a value.
; Notes:
; For an enabled expression, propagate var values aka bind-var
;   then expression value is derived from the bound vars.
;      For the var that was just bound, it will be according to the recently bound var
;      If all the vars are already bound, then expression value is constrained by the bounding vars. Recall in between constraints
;   Then we check if the enabled constraint satisfies the constraint.