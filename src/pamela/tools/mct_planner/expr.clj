;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

; General notes
; constraint expressions (make-constraint-expression) created from nodes are marked as controllable = true
; but not for activities. Because expressions coming from nodes are over arching constraints that need to be satisfied
; In case of failures, planner can suggest options to make the tpn a success

(ns pamela.tools.mct-planner.expr
  (:gen-class)
  (:require
    [pamela.tools.mct-planner.util :refer :all]
    [pamela.tools.mct-planner.weighted-probability :as wp]
    [pamela.tools.mct-planner.learning :as mlearn]
    [pamela.tools.utils.tpn-json :as tpn-json]
    [pamela.tools.utils.util :as util]
    [clojure.pprint :refer :all]
    [clojure.set :as set])
  )

(defprotocol exprI
  (vars [expr]
    "Return all vars of the expression")
  (vars-bound? [expr bindings]
    "Returns true if all vars of the expression are bound, including the value false for a var.
    i.e each var exists in bindings and has a non nil value.")
  (from-vars-bound? [expr bindings]
    "Returns true if all the from-vars are bound.
    i.e each var has a non nil value, including false")
  (to-var-bound? [expr bindings]
    "Return true if the to-var is bound.
    i.e to-var has a non nil value, including false")
  (from-values [expr m]
    "Return values of from-vars")
  (is-range-constraint? [expr]
    "Return true if this expression has anything to do with range constraints.")
  (get-tpn-uid [expr]
    "Return TPN object uid associated with the expr"))

(defrecord expr [symbol to-var from-vars value controllable m]
  exprI
  (vars [expr]
    (conj (:from-vars expr) (:to-var expr)))
  (vars-bound? [expr bindings]
    (check-bindings (vars expr) bindings))
  (from-vars-bound? [expr bindings]
    (check-bindings (:from-vars expr) bindings))
  (to-var-bound? [expr bindings]
    (check-bindings [(:to-var expr)] bindings))
  (from-values [expr mlocal]
    (vals (select-keys mlocal (:from-vars expr))))
  (is-range-constraint? [expr]
    (is-range-var? (:to-var expr)))
  (get-tpn-uid [expr]
    (get-in expr [:m :object])))

(defn get-vars [exprs]
  "Given a list of exprs, return a set of all vars"
  (reduce (fn [result expr]
            (into result (vars expr)))
          #{} exprs))

(defn get-to-vars [exprs]
  "Given a list of expressions, return a set to-vars"
  (reduce (fn [result expr]
            (conj result (:to-var expr)))
          #{} exprs))

(defn get-begin-vars [exprs]
  "Will return vars that do not appear in fromvars list of any expression"
  (set/difference (get-vars exprs) (get-to-vars exprs)))

(defn get-range-vars [exprs]
  "return a set of all vars that participate in range constraints."
  (filter is-range-var? (get-vars exprs)))

(defn get-initial-bindings [exprs]
  ;(println "get-initial-bindings")
  (reduce (fn [result var]
            (if-not (is-select-var? var)
              (conj result {var 0})
              result))
          {} (get-begin-vars exprs)))

(defn as-list [expr]
  "Return a list of the form (fn-symbol to-var from-var optional-value :controllable t/f)"
  (let [                                                    ;_ (do (println "as-list m keys" (keys (:m expr))))
        partial (list (dissoc (:m expr) :monte-learner))    ;Do not wish to print monte learner object and associated details here.
        ;partial (list (:m expr))
        controllable (:controllable expr)
        partial (if controllable
                  (conj partial controllable :controllable)
                  partial)
        value (:value expr)
        partial (if value (conj partial value) partial)
        from-vars (if (-> (count (:from-vars expr))
                          (= 1))
                    (first (:from-vars expr))
                    (:from-vars expr))]
    (conj partial from-vars (:to-var expr) (:symbol expr))))

; Useful methods for pretty printing expressions
#_(defmethod print-method expr [expr ^java.io.Writer w]
    (print-method (as-list expr) w))

(defn expr-pprint [expr]
  (pr (as-list expr)))

(. clojure.pprint/simple-dispatch addMethod expr expr-pprint)

(defmulti make-constraint-expr
          "Args: constraint obj from-obj controllable m"
          (fn [constraint _ _ _ _]
            (:tpn-type constraint)))

(defmethod make-constraint-expr :default
  [constraint obj from-obj controllable extra-m]
  (binding [*out* *err*]
    (println "make-constraint-expr impl for" (:tpn-type constraint))
    (println "constraint")
    (pprint constraint)
    (println "obj")
    (pprint obj)
    (println "from-obj")
    (pprint from-obj)
    (println "controllable" controllable)
    ;(println "full-tpn-map")
    ;(pprint extra-m)
    )
  (throw (Exception. (str "make-constraint-expr impl for " (:tpn-type constraint)))))

(defn add-display-name [to-obj obj]
  (if (:display-name obj)
    (conj to-obj {:display-name (:display-name obj)})
    to-obj))

(defn convert-bounds-to-vals [bounds]
  (into [] (range (first bounds) (+ 1 (second bounds)))))

(defn make-learner [bounds options-mp]
  (if (or (= java.lang.Double/POSITIVE_INFINITY (first bounds))
          (= java.lang.Double/POSITIVE_INFINITY (second bounds)))
    (do (util/to-std-err (println "Monte carlo learner does not yet support infinity values")))
    (let [vals (convert-bounds-to-vals bounds)]
      ;(println "make-monte" vals options-mp)
      (mlearn/make-monte-learner vals options-mp))))

(defmethod make-constraint-expr :cost<=-constraint [constraint obj from-node controllable m]
  (let [end-node (util/get-end-node constraint m)
        max-cost (:value constraint)
        m-learn (if (and controllable (contains? m :monte-config))
                  (make-learner [0 max-cost] (:monte-config m)))
        expr-m (if m-learn {:object (:uid obj) :from-nodes #{(:uid from-node)}
                            :temporal-constraint (:uid constraint) :monte-learner m-learn}
                           {:object (:uid obj) :from-nodes #{(:uid from-node)}
                            :temporal-constraint (:uid constraint)})
        ]
    (->expr 'max-cost
            (:cost-var end-node) #{(:cost-var from-node)} max-cost
            controllable
            (add-display-name expr-m obj))))

(defmethod make-constraint-expr :reward>=-constraint [constraint obj from-node controllable m]
  ;(pprint m)
  ;(println "keys of m" (keys m))
  ;(println "from node")
  ;(pprint from-node)
  ;(println "obj")
  ;(pprint obj)
  ;(println "constraint")
  ;(pprint constraint)
  ;(println "static uncertainty")
  ;(pprint (get-in m [:static-uncertainty (:end-node constraint)]))
  (let [end-node (util/get-end-node constraint m)
        min-reward (:value constraint)
        ; For efficiency reasons baking in max-reward.
        ; ideally, we would be learning for reward values as and when they arrive and not constraining
        ; to range bounded by min/max rewards FIXME
        ;max-reward (* 2 min-reward)
        reward-bounds (get-in m [:static-uncertainty (:end-node constraint) :reward-bounds])
        max-reward (second reward-bounds)
        m-learn (if (and controllable (contains? m :monte-config))
                  (make-learner [min-reward max-reward] (:monte-config m)))
        expr-m {:object (:uid obj) :from-nodes #{(:uid from-node)}
                :temporal-constraint (:uid constraint) :max-reward max-reward}
        expr-m (if m-learn (conj expr-m {:monte-learner m-learn}
                          expr-m))
        ]
    (->expr 'min-reward
            (:reward-var end-node) #{(:reward-var from-node)} min-reward
            controllable
            (add-display-name expr-m obj))))

(defmethod make-constraint-expr :temporal-constraint
  [constraint obj from-node controllable m]
  (let [end-node (util/get-end-node constraint m)
        bounds (:value constraint)
        m-learn (if (and controllable (contains? m :monte-config))
                  (make-learner bounds (:monte-config m)))
        expr-m (if m-learn {:object (:uid obj) :from-nodes #{(:uid from-node)}
                            :temporal-constraint (:uid constraint) :monte-learner m-learn}
                           {:object (:uid obj) :from-nodes #{(:uid from-node)}
                            :temporal-constraint (:uid constraint)})
        ]
    (->expr 'in-range
            (:range-var end-node) #{(:range-var from-node)} bounds
            controllable
            (add-display-name expr-m obj))))

(defn make-constraint-exprs [obj from-obj constraints controllable m]
  "Returns a vector of expressions"
  (loop [cntrs constraints
         exprs []]
    (if-not (first cntrs)
      exprs
      (recur (rest cntrs) (conj exprs (make-constraint-expr (util/get-object (first cntrs) m)
                                                            obj
                                                            from-obj
                                                            controllable
                                                            m))))))

(defn add-vars [node counter]
  "Adds vars for each expression type such as cost, reward and range
  return {:node updated-node :var-m var-to-node-id-m}"
  (let [generic-var (keyword (str "v" counter))
        range-var (keyword (str "v" counter "-range"))
        cost-var (keyword (str "v" counter "-cost"))
        reward-var (keyword (str "v" counter "-reward"))
        nid (:uid node)
        select-var (keyword (str "v" counter "-select"))]
    {:node  (merge node {:range-var  range-var
                         :cost-var   cost-var
                         :reward-var reward-var
                         :var        generic-var
                         :select-var select-var})
     :var-m (conj {range-var nid cost-var nid reward-var nid generic-var nid select-var nid})}))

;;
(defn add-node-vars [m n-order]
  "m is TPN map, n-order is some order for nodes"
  (loop [m-nvars m                                          ;m with node vars added
         var-m {}                                           ;map with vars as keys and node uid as value
         counter 0]
    (if (= counter (count n-order))
      {:m m-nvars :var-2-nid var-m}
      (let [nid (nth n-order counter)
            node (nid m)
            vars-added (add-vars node counter)]
        (recur (assoc m-nvars nid (:node vars-added))
               (merge var-m (:var-m vars-added))
               (inc counter))))))

; Making constraints
; for each in bfs order,
; for node, make temporal constraint, cost=, reward=, cost<=, reward>=
; for each activity, make temporal constraint, cost= and reward=
;; Need to support cost, reward and other constraints in addition to range-constraints.

(defn controllable?
  "Return true when controllabe key exists and has true value.
   Return false otherwise." [obj]
  (and (contains? obj :controllable)
       (= true (:controllable obj))))

(defmulti make-object-expressions
          "Dispatch fn to create constraint expressions
          [obj from-obj m]
          Objects should be a tpn map"
          (fn [obj _ _]
            (:tpn-type obj)))

(defn make-activities-expr [acts from-obj m]
  "Returns a vector of expressions"
  (loop [l-acts acts
         result []]
    (if (empty? l-acts)
      result
      (recur (rest l-acts) (into result (make-object-expressions (util/get-object (first l-acts) m)
                                                                 from-obj m)))
      )))

(defmethod make-object-expressions :default [obj _ _]
  (binding [*out* *err*]
    (println "make-object-expressions: need to implement for :tpn-type" (:tpn-type obj))
    (pprint obj)))

(defmethod make-object-expressions :p-begin [obj from-obj m]
  (let [exprs (make-constraint-exprs obj from-obj (:constraints obj) true m)
        exprs (into exprs (make-activities-expr (:activities obj) from-obj m))]
    exprs))

(defn make-max-constraints [pend-obj m]
  "Given a p-end node and incidence set, create max constraints.
  Returns a vector of constraints"
  (let [from-nodes (map (fn [a-id]
                          (-> a-id
                              (util/get-object m)
                              :from-node
                              (util/get-object m))
                          ) (:incidence-set pend-obj))
        from-node-uids (into #{} (map :uid from-nodes))
        range-vars (into #{} (map (fn [obj]
                                    (:range-var obj)
                                    ) from-nodes))

        cost-vars (into #{} (map (fn [obj]
                                   (:cost-var obj)
                                   ) from-nodes))

        reward-vars (into #{} (map (fn [obj]
                                     (:reward-var obj)
                                     ) from-nodes))
        exprs [(->expr 'in-range-max (:range-var pend-obj) range-vars nil false
                       {:object (:uid pend-obj) :from-nodes from-node-uids})
               (->expr 'cost-max (:cost-var pend-obj) cost-vars nil false
                       {:object (:uid pend-obj) :from-nodes from-node-uids})
               (->expr 'reward-max (:reward-var pend-obj) reward-vars nil false
                       {:object (:uid pend-obj) :from-nodes from-node-uids})
               ]
        ]
    exprs))

(defn make-<=-constraint-for-pend [p-end m]
  (reduce (fn [exprs act-id]
            (let [from-node (-> act-id (util/get-object m) :from-node (util/get-object m))]
              (into exprs [(->expr 'range<= (:range-var from-node) #{(:range-var p-end)} nil false
                                     {:object act-id :from-nodes #{(:uid from-node)}})
                           (->expr 'cost<= (:cost-var from-node)  #{(:cost-var p-end)} nil false
                                     {:object act-id :from-nodes #{(:uid from-node)}})
                           (->expr 'reward<= (:reward-var from-node)  #{(:reward-var p-end)} nil false
                                     {:object act-id :from-nodes #{(:uid from-node)}})])))
          [] (:incidence-set p-end)))

(defmethod make-object-expressions :p-end [obj from-obj m]
  (let [exprs (make-constraint-exprs obj from-obj (:constraints obj) true m)
        exprs (into exprs (make-activities-expr (:activities obj) from-obj m))
        exprs (into exprs (make-max-constraints obj m))
        exprs (into exprs (make-<=-constraint-for-pend obj m))]
    exprs))

(defmethod make-object-expressions :null-activity [obj from-obj m]
  (let [end-node (util/get-end-node obj m)
        ; We do not need expressions for null activities whose end node is p-end because '= expresions conflict with in-range-max expression.
        ; So we selectively create '= expressions.
        ; By not creating = expressions for p-end nodes, we are effectively blocking propagation of values to p-end node.
        ; Instead we create in-range-max, cost-max and reward-max to propagate values to p-end node.
        ; However there is a case (see controllable-parallel.pamela which has a controllable activity and over arching constraint whose
        ; bounds are equal. Which leads to a case where we do need to propagate value from p-end to other node (which is
        ; end node of another controllable activity)
        ; To allow propagation for this case, we add begin-var of null activity <= end-var (for p-end node only)
        exprs (if (not= :p-end (:tpn-type end-node))
                [(->expr '= (:range-var end-node) #{(:range-var from-obj)} nil false
                         {:object (:uid obj) :from-nodes #{(:uid from-obj)}})
                 (->expr '= (:cost-var end-node) #{(:cost-var from-obj)} nil false
                         {:object (:uid obj) :from-nodes #{(:uid from-obj)}})
                 (->expr '= (:reward-var end-node) #{(:reward-var from-obj)} nil false
                         {:object (:uid obj) :from-nodes #{(:uid from-obj)}})]
                [])]
    exprs))

(defmethod make-object-expressions :state [obj from-obj m]
  (let [exprs (make-constraint-exprs obj from-obj (:constraints obj) true m)
        exprs (into exprs (make-activities-expr (:activities obj) from-obj m))
        ] exprs))

(defn make-cost-reward-constraint
  "Helper function to create cost or reward constraint"
  [sym to-var from-vars value controllable expr-m monte-config]
  (let [m-learn (if (and controllable monte-config)
                  (make-learner [0 value] monte-config))
        m (if m-learn (conj expr-m {:monte-learner m-learn})
                      expr-m)]
    (->expr sym to-var from-vars value controllable m)))

(defmethod make-object-expressions :activity [obj from-obj m]
  (let [end-node (util/get-end-node obj m)
        controllable (controllable? obj)
        exprs (make-constraint-exprs obj from-obj (:constraints obj) controllable m)
        expr-m (add-display-name {:object (:uid obj) :from-nodes #{(:uid from-obj)}}
                                 obj)

        cost (if (:cost obj)
               (:cost obj)
               0)

        cost-constraint (make-cost-reward-constraint 'cost= (:cost-var end-node) #{(:cost-var from-obj)} cost false expr-m (:monte-config m))

        reward (if (:reward obj)
                 (:reward obj)
                 0)

        reward-constraint (make-cost-reward-constraint 'reward= (:reward-var end-node) #{(:reward-var from-obj)} reward false expr-m (:monte-config m))
        exprs (conj exprs cost-constraint reward-constraint)

        has-temporal? (some (fn [expr]
                              (= (:symbol expr) 'in-range)) exprs)

        exprs (if-not has-temporal?
                (conj exprs (->expr 'in-range (:range-var end-node) #{(:range-var from-obj)}
                                    [0 java.lang.Double/POSITIVE_INFINITY] controllable expr-m))
                exprs)
        ]
    exprs))

(defmethod make-object-expressions :delay-activity [obj from-obj m]
  (let [controllable (controllable? obj)
        exprs (make-constraint-exprs obj from-obj (:constraints obj) controllable m)
        ]
    exprs))


(defn get-cbegin-sequence-node-pairs [cnode m]
  "Given cbegin-node, find begin-node and end-node of each sequence.
   begin-node of a sequence is the end-node of the outgoing null-activity.
   end-node of a sequence is the begin-node of null-activity whose end-node is
   cend-node. Return list of [begin end]"
  (map (fn [act-id]
         (let [seq-begin-node (util/get-end-node (util/get-object act-id m) m)
               ; TPN files generated via HTN do not have :sequence-end node. Worse, for each state node, it has :end-node to be the :end-node of the
               ; outgoing activity.
               ;seq-end-node (or (:sequence-end seq-begin-node) (:end-node seq-begin-node))
               path (util/find-any-path (:uid seq-begin-node) (:end-node cnode) m)
               seq-end-node (last (butlast (butlast path))) ;3rd last element
               ]
           (when-not (last path)
             (println "Error in get-cbegin-sequence-node-pairs: complete path not found for (:uid seq-begin-node) (:end-node cnode)"))
           [(:uid seq-begin-node) seq-end-node]))
       (:activities cnode)))

(defmethod make-object-expressions :c-end [obj from-obj m]
  (let [exprs (make-constraint-exprs obj from-obj (:constraints obj) true m)
        exprs (into exprs (make-activities-expr (:activities obj) from-obj m))]
    exprs))

(defn make-if=exprs [avar pair c-begin-node to-node from-node m]
  (let [e (->expr 'if=                                      ;symbol
                  (avar to-node)                            ;to-var
                  #{(avar from-node)}                       ;from-vars
                  [(:select-var c-begin-node) (-> (first pair) (util/get-object m) (:select-var))] ;value
                  false                                     ;controllable
                  {:object (:uid c-begin-node) :from-nodes #{(:uid from-node)}} ;m
                  )]
    (when (nil? (avar from-node))
      (println "Error" avar)
      (pprint from-node)
      (println "pair m" pair)
      (pprint m)
      (pprint e)) e))

(defn extract-static-uncertainty
  "Associate static uncertainty for each outgoing path
  Find select-var for each outgoing null-activity of choice node
  and return static uncertainty from tpn"
  [c-node tpn]
  (into {} (map (fn [id]
                  {(-> id (util/get-object tpn) :end-node (util/get-object tpn) :select-var) (-> tpn :static-uncertainty id)})
                (:activities c-node))))

(defmethod make-object-expressions :c-begin [obj from-obj m]
  ;(println "make-object-expressions :c-begin monte-config" (:monte-config m))
  (let [exprs (make-constraint-exprs obj from-obj (:constraints obj) true m)
        end-node (util/get-end-node obj m)
        end-var (:range-var end-node)
        seq-begin-end-nodes (get-cbegin-sequence-node-pairs obj m)
        ; end-node-vars of outgoing null-activities
        seq-begin-vars (into #{} (map (fn [pair]
                                        (-> pair (first) (util/get-object m) (:select-var))
                                        ) seq-begin-end-nodes))
        unc (extract-static-uncertainty obj m)
        weighted-distribution (wp/make-distribution (reduce (fn [res [var {weight :weight}]]
                                                                                     (conj res {var weight}))
                                                                                   {} unc))
        m-learn (mlearn/make-monte-learner (into [] seq-begin-vars) (conj (:monte-config m) {:pdf-for :choice}))
        exprs (conj exprs
                    (->expr 'selector= (:select-var obj) seq-begin-vars nil true
                            ; we use range-end-var to support choosing a path based on uncertainty
                            {:object (:uid obj) :from-nodes #{(:uid from-obj)} :range-end-var end-var
                             :static-uncertainty unc :weighted-distribution weighted-distribution
                             :monte-learner m-learn}))

        exprs (into exprs (flatten (map (fn [pair]
                                          [(make-if=exprs :range-var pair obj (util/get-object (first pair) m) obj m)
                                           (make-if=exprs :cost-var pair obj (util/get-object (first pair) m) obj m)
                                           (make-if=exprs :reward-var pair obj (util/get-object (first pair) m) obj m)])
                                        seq-begin-end-nodes)))]
    exprs))

(defn make-expressions [m bfs-order]
  (loop [in-order bfs-order
         exprs []]
    (if-not (first in-order)
      exprs
      (recur (rest in-order) (into exprs (make-object-expressions (util/get-object (first in-order) m)
                                                                  (util/get-object (first in-order) m)
                                                                  m))))))

(defn update-from-nodes [m nodes]
  "For all activities object, add (:from-node (:uid from-node)) pair"
  (loop [new-m m
         lnodes nodes]
    (if (empty? lnodes)
      new-m
      (let [node (util/get-object (first lnodes) m)
            acts (into {} (map (fn [a-id]
                                 {a-id (conj (a-id m) {:from-node (:uid node)})})
                               (:activities node)))]
        (recur (merge new-m acts)
               (rest lnodes))))))

(defn replace-expr-vars [expr eq-m]
  (let [to-var (:to-var expr)
        from-vars (:from-vars expr)
        new-expr (if (eq-m to-var)
                   (assoc expr :to-var (eq-m to-var))
                   expr)
        new-expr (assoc new-expr :from-vars (into #{} (replace eq-m from-vars)))]
    new-expr))

(defn replace-eq-vars2 [exprs eq-m]
  "For each expression, replace :to-var and :from-var vars according to given map
  i.e if the any of the var in expression exists in eq-m as key, replace it with value in eq-m"
  (loop [l-exprs exprs
         new-exprs []]
    (if (empty? l-exprs)
      new-exprs
      (recur (rest l-exprs)
             (conj new-exprs (replace-expr-vars (first l-exprs) eq-m))))))

(defn get-equal-vars [exprs]
  "Return a list of sets where each inner set contains equal vars."
  (remove nil? (map (fn [expr]
                      (if (= '= (:symbol expr))
                        (conj (:from-vars expr) (:to-var expr))))
                    exprs)))

(defn qa-check-expr [e]
  (when (nil? (:to-var e))
    (println "qa-check-expr Got to-var nil" e))
  (when (and (controllable? e) (nil? (get-in e [:m :monte-learner]))
             (if (and (= 'in-range (:symbol e))
                      (== java.lang.Double/POSITIVE_INFINITY (second (:value e)))) ; Note: in-range with ub = Inf does not has monte-learner
               false
               true))
    ;(println "monte" (get e [:m :monte-learner]))
    (util/to-std-err (println "qa-check-expr " (:symbol e) "controllable expr does not has monte-learner")
                     (println e))))

(defn qa-check-exprs [exprs]
  (doseq [e exprs]
    ;(pprint e)
    (qa-check-expr e)))

(defn make-obj-2-vars [exprs]
  (reduce (fn [result expr]
            ;(println "reducing" expr)
            ;(pprint result)
            (let [uid (get-in expr [:m :object])]
              ;(println uid)
              (if-not (contains? result uid)
                (conj result {uid {:from-vars (:from-vars expr) :to-vars #{(:to-var expr)}}})
                (conj result (update-in result [uid] (fn [old-value]
                                                       (println "old-value" old-value)
                                                       (println "new-value from-vars" (into (:from-vars old-value) (:from-vars expr)))
                                                       {:from-vars (into (:from-vars old-value) (:from-vars expr))
                                                        :to-vars   (conj (:to-vars old-value) (:to-var expr))})))
                )
              )) {} exprs))

(defn make-nid-2-var [var-2-nid]
  (reduce (fn [result [k v]]
            (if-not (contains? result v)
              (conj result {v #{k}})
              (conj result {v (conj (v result) k)})
              )
            ) {} var-2-nid))

(defn update-expr-m [expr key value]
  (conj expr {:m (assoc (:m expr) key value)}))

(defn add-tpn-bounds
  "Given list of expressions, update :m with :tpn-bounds"
  [exprs tpn-bounds]
  (reduce (fn [res expr]
            (conj res (update-expr-m expr :tpn-bounds tpn-bounds))) [] exprs))

(defn add-propagated [exprs propagated]
  ;(println "propagated")
  ;(pprint propagated)
  (reduce (fn [res expr]
            (let [obj-id (get-in expr [:m :object])
                  propagated-value (get propagated obj-id)]
              (cond (nil? obj-id)
                    (throw (Exception. (str "add-propagated " expr " has nil :object")))
                    (nil? propagated-value)
                    (throw (Exception. (str "add-propagated " expr " has nil propagated value for object " obj-id)))
                    :else
                    (conj res (update-expr-m expr :propagated propagated-value)))))
          [] exprs))

(defn make-expressions-from-map [m]
  (let [in-bfs (bfs-walk-nodes m)
        m (update-from-nodes m in-bfs)
        vars-added (add-node-vars m in-bfs)
        {m :m var-2-nid :var-2-nid} vars-added
        static-uncertainty (propagate-static-uncertainty m)
        propagated (propagate-time-cost-reward m)
        tpn-bounds (get-tpn-time-cost-reward propagated m)
        ;_ (println "Static Unc")
        ;_ (pprint static-uncertainty)
        m (merge m {:static-uncertainty static-uncertainty
                    :monte-config (mlearn/make-default-mlearner-options)
                    })
        nid-2-var (make-nid-2-var var-2-nid)
        m (conj m {:nid-2-var nid-2-var})
        all-exprs (make-expressions m in-bfs)
        all-exprs (add-propagated all-exprs propagated)
        all-exprs (add-tpn-bounds all-exprs tpn-bounds)
        ;object-2-vars (make-obj-2-vars all-exprs)
        _ (qa-check-exprs all-exprs)
        ;eq-vars (get-equal-vars all-exprs)
        ;eq-classes (into-equivalent-classes eq-vars)
        ;ordrd-eq-classes (ordered-eq-classes eq-classes var-2-nid m)
        ;eq-classes-m (eq-classes-to-map ordrd-eq-classes)
        ;all-exprs-equal-removed (remove (fn [expr]
        ;                                  (= '= (:symbol expr))) all-exprs)
        ;exprs (replace-eq-vars2 all-exprs-equal-removed eq-classes-m)
        ]

    ;(println "expressions:")
    ;(pprint all-exprs)
    ;(println "equivalent vars")
    ;(pprint eq-vars)
    ;(println "equivalent classes")
    ;(pprint ordrd-eq-classes)
    ;(println "eq classes m")
    ;(pprint eq-classes-m)
    ;(println "vars replaced")
    ;(pprint exprs)

    {
     :node-bfs  in-bfs                                      ;node ids in bfs order
     :all-exprs all-exprs                                   ;original set of expressions derived from tpn
     ;:equal-vars            eq-vars                         ;vars of equal expressions
     ;:equal-classes         eq-classes                      ;equal vars binned into equivalence classes
     ;:ordered-equal-classes ordrd-eq-classes                ;equivalence classed ordered according to some criteria
     ;:exprs                 exprs                           ;expressions after removing equal-vars
     :var-2-nid var-2-nid
     :nid-2-var nid-2-var
     }))

(defn make-expressions-from-file [file]
  "Input is TPN as JSON. Return map containing expressions along with intermediate data useful for testing."
  (make-expressions-from-map (tpn-json/from-file file)))

(defn save-tpn-expression-info [from-file to-file]
  (save-clj-data (into (sorted-map) (make-expressions-from-file from-file))
                 to-file))

; Set of functions to make expr object from a list

(defmulti make-expr-from-list
          "Inverse of as-list implemented above."
          (fn [el]
            (first el)))

(defmethod make-expr-from-list :default [el]
  (util/to-std-err (println "Impl make-expr-from-list" (first el) el)))

(defmethod make-expr-from-list 'in-range [el]
  "in-range expressions are :controllable or not
  If they are :controllable then, keyword and value are at index 4 and 5."
  (let [len (count el)
        sym (first el)
        tovar (second el)
        fromvar (nth el 2)                                  ;third
        val (nth el 3)
        val (if (number? (second val))
              val
              [(first val) java.lang.Double/POSITIVE_INFINITY])
        controllable (if (> len 5)
                       (nth el 5)
                       false)
        m (nth el (dec len))]
    (->expr sym tovar #{fromvar} val controllable m)))

(defn make-max-expr [el]
  (let [sym (first el)
        tovar (second el)
        fromvars (nth el 2)
        fromvars (if (instance? clojure.lang.Keyword fromvars)
                   #{fromvars}
                   fromvars)

        m (nth el 3)]

    #_(println sym tovar fromvars m "from-vars types" (type (nth el 2)) (type fromvars))
    (->expr sym tovar fromvars nil false m)))

(defmethod make-expr-from-list 'in-range-max [el]
  (make-max-expr el))

(defmethod make-expr-from-list 'cost-max [el]
  (make-max-expr el))

(defmethod make-expr-from-list 'reward-max [el]
  (make-max-expr el))

(defn make-equal-expr-value [el]
  "Parses an equal expression with value"
  (let [sym (first el)
        tovar (second el)
        fromvars #{(nth el 2)}
        value (nth el 3)
        len (count el)
        controllable (if (> len 5)
                       (nth el 5)
                       false)
        m (nth el (dec len))]
    (->expr sym tovar fromvars value controllable m)))

(defmethod make-expr-from-list '= [el]
  (let [sym (first el)
        tovar (second el)
        fromvars #{(nth el 2)}
        len (count el)
        controllable (if (> len 4)
                       (nth el 4)
                       false)
        m (nth el (dec len))]
    (->expr sym tovar fromvars nil controllable m)))

(defmethod make-expr-from-list 'cost= [el]
  (make-equal-expr-value el))

(defmethod make-expr-from-list 'reward= [el]
  (make-equal-expr-value el))

(defmethod make-expr-from-list 'selector= [el]
  (->expr (first el) (second el) (nth el 2) nil (nth el 4) (nth el 5)))

(defmethod make-expr-from-list 'if= [el]
  (->expr (first el) (second el) #{(nth el 2)} (nth el 3) false (nth el 4)))

(defmethod make-expr-from-list 'range<= [el]
  (->expr (first el) (second el) #{(nth el 2)} nil false (nth el 3) ))

(defmethod make-expr-from-list 'cost<= [el]
  (->expr (first el) (second el) #{(nth el 2)} nil false (nth el 3) ))

(defmethod make-expr-from-list 'reward<= [el]
  (->expr (first el) (second el) #{(nth el 2)} nil false (nth el 3) ))

(defn expr-list-to-records [exprs]
  (into [] (map (fn [expr]
                  (make-expr-from-list expr)
                  ) exprs)))