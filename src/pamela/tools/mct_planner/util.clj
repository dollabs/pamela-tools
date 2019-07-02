;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.util
  (:require [pamela.tools.utils.util :refer :all]
            [pamela.tools.utils.tpn-types :as tpn_type]
            [clojure.pprint :refer :all]
            [clojure.set :as set]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.core.matrix.operators :as m-op]
            )
  (:import [java.io PushbackReader]
           [org.apache.commons.math3.distribution EnumeratedDistribution]
           [org.apache.commons.math3.util Pair]
           ))


(defn is-range-var? [var]
  {:pre [(not (nil? var))]}
  (string/ends-with? var "-range"))

(defn is-cost-var? [var]
  {:pre [(not (nil? var))]}
  (string/ends-with? var "-cost"))

(defn is-reward-var? [var]
  {:pre [(not (nil? var))]}
  (string/ends-with? var "-reward"))

(defn is-select-var? [var]
  {:pre [(not (nil? var))]}
  (string/ends-with? var "-select"))

(defn get-vars-as-list [expr]
  "Given a expression, return the list of vars in the expression maintaining order of appearance"
  (cond (= 'in-range (first expr))
        (list (second expr) (nth expr 2))
        (= 'in-range-max (first expr))
        (cons (second expr) (nth expr 2))
        :else
        (binding [*out* *err*]
          (println "get-vars-as-list unknown expression" expr))))

(defn get-vars-old [expr]
  "Given a expression, return the set of vars in the expression"
  (into #{} (get-vars-as-list expr)))

(defn cost-valid? [obj]
  (and (:cost obj) (> (:cost obj) 0)))

(defn reward-valid? [obj]
  (and (:reward obj) (> (:reward obj) 0)))

(defn save-clj-data [data file]
  (let [sorted (into (sorted-map) data)]
    (with-open [writr (clojure.java.io/writer file)]
      (binding [*out* writr]
        (pprint sorted)))))

(defn read-clj-data [file]
  (with-open [rdr (PushbackReader. (clojure.java.io/reader file))]
    (binding [*read-eval* false]
      (read rdr))))

(defn bfs-walk
  "Walk TPN in BFS order and return a vector of [:obj-uid {:from :from-obj-uid}] of all objects
  Note: For c-end and p-end nodes there will be only one :from-obj-uid (the first one found in bfs order)"
  [tpn]
  (let [begin (:begin-node (get-network-object tpn))
        visited (atom #{})
        visit-fn (fn [obj]
                   (cond (contains? tpn_type/nodetypes (:tpn-type obj))
                         (map (fn [act-id]
                                [act-id {:from (:uid obj)}]) (:activities obj))
                         (contains? tpn_type/edgetypes (:tpn-type obj))
                         [[(:end-node obj) {:from (:uid obj)}]]
                         :else
                         (to-std-err
                           (println "bfs-walk unknown object" obj))))
        visit-and-add-fn (fn [obj myq]
                           (let [more-objs (visit-fn obj)]
                             (if (pos? (count more-objs))
                               (into myq more-objs)
                               myq)))]
    (loop [obj-uid [begin {:from nil}]
           result []
           myq clojure.lang.PersistentQueue/EMPTY]
      (if (nil? obj-uid)
        result
        (if-not (contains? @visited (first obj-uid))
          (let [obj (get-object (first obj-uid) tpn)]
            (cond (contains? tpn_type/edgetypes (:tpn-type obj))
                  (let [updated-q (visit-and-add-fn obj myq)]
                    (swap! visited conj (first obj-uid))
                    (recur (first updated-q) (conj result obj-uid) (pop updated-q)))

                  (empty? (set/difference (:incidence-set obj) @visited))
                  (let [updated-q (visit-and-add-fn obj myq)]
                    (swap! visited conj (first obj-uid))
                    (recur (first updated-q) (conj result obj-uid) (pop updated-q)))

                  :else
                  (recur (first myq) result (pop myq))))
          (recur (first myq) result (pop myq)))))))

(defn bfs-walk-nodes [m]
  "Walk TPN in Breadth first search order and return a vector of nodes"
  (let [begin (:begin-node (get-network-object m))
        visited (atom #{})
        fn-end-nodes (fn [id]
                       (map (fn [act]
                              (:end-node (get-object act m)))
                            (:activities (get-object id m))))]

    (loop [obj begin
           result (conj [] begin)
           myq clojure.lang.PersistentQueue/EMPTY]
      (let [enodes (fn-end-nodes obj)
            enodes (filter (fn [x]
                             (not (contains? @visited x))) enodes)
            myq (into myq enodes)]
        (swap! visited into enodes)
        (if-not (first myq)
          (into [] (distinct result))
          (recur (first myq) (conj result (first myq)) (pop myq)))))))

(defn update-equivalent-classes [classes set]
  "classes is a vector of equivalent classes.
   Update the element(a-class) of classes containing any element of set. i.e (union set a-class)
   or conj the set to classes"
  (let [at-index (keep-indexed (fn [index item]
                                 #_(println "item" item "set" set (set/intersection item set))
                                 (if (not-empty (set/intersection item set)) index nil))
                               classes)]

    (when (> (count at-index) 1)
      (println "ERROR: Multiple elements in classes match set")
      (println "classes" classes)
      (println "set" set)
      (println "at-indices of classes" at-index))

    ; elements of classes must be disjoint
    (if (not-empty at-index)
      (let [to-update (get classes (first at-index))
            to-update (set/union to-update set)]
        ;(println "updated at index" (first at-index) to-update)
        (assoc classes (first at-index) to-update))
      (conj classes set))))

(defn into-equivalent-classes [eq-vars]
  "eq-vars is list of sets. Return a list of disjoint sets where each element contains equivalent vars."
  (loop [classes []
         remaining eq-vars]
    (if-not (first remaining)
      classes
      (recur (update-equivalent-classes classes (first remaining)) (rest remaining)))))

(defn get-preferred-order [var-id var-2-nid m]
  "For a given var-id, return it's preferred order"
  (let [uid (var-id var-2-nid)
        obj (get-object uid m)]
    ((:tpn-type obj) tpn_type/equivalent-class-preferences)))

(defn ordered-eq-classes [eq-classes var-2-nid m]
  "Return equivalant classes with each class ordered according to tpn-type/equivalent-class-preferences"
  (loop [ordered []
         remaining eq-classes]
    (if-not (first remaining)
      ordered
      (recur (conj ordered (sort-by (fn [x]
                                      (get-preferred-order x var-2-nid m))
                                    (first remaining)))
             (rest remaining)))))

(defn eq-vars-to-map [ordrd-vars]
  "Given a list of ordered vars, assume all vars are equal to first of ordrd-vars"
  (loop [updated {(first ordrd-vars) (first ordrd-vars)}
         remaining (rest ordrd-vars)]
    (if-not (first remaining)
      updated
      (recur (merge updated {(first remaining) (first ordrd-vars)}) (rest remaining)))))

(defn eq-classes-to-map [eq-classes]
  "Convert disjoint set of equivalent classes to map"
  (let [m (loop [updated {}
                 remaining eq-classes]
            (if-not (first remaining)
              updated
              (recur (conj updated (eq-vars-to-map (first remaining))) (rest remaining))))]
    ; Remove where key = value
    (into {} (filter (fn [[k v]]
                       (if-not (= k v) true))
                     m))))

(defn get-bound-vars [bindings]
  "Given bindings, return set of vars that have non nil values"
  (into #{} (remove nil? (map (fn [[k v]]
                                (if v k nil))
                              bindings))))

(defn check-bindings [vars bindings]
  "Given a set of vars and bindings, return true if all the vars have a valid (non nil) value in bindings."
  (every? (fn [var]
            (if-not (nil? (get bindings var))
              true false)) vars))

(defn var-bound? [var bindings]
  (if-not (nil? (get bindings var))
    true false))

(defn duration [bounds]
  "Assumes bounds is [lb ub]
   returns ub - lb + 1"                                     ;Wrong place to add 1.
  (let [[lb ub] bounds
        lb (if (= lb java.lang.Double/NEGATIVE_INFINITY)
             java.lang.Integer/MIN_VALUE
             lb)
        ub (if (= ub java.lang.Double/POSITIVE_INFINITY)
             java.lang.Integer/MAX_VALUE
             ub)]
    (- ub lb)))                                             ;Removing +1 on 7/18/2018 - PM

(defn add-bounds [a b]
  ;(println a "add-bounds" b)
  [(+ (first a) (first b))
   (+ (second a) (second b))])

(defn probability-distribution
  "Returns flat probability distribution of each integral value
  in bounds inclusive of lb and ub"
  [bounds]
  (/ 1 (+ 1 (duration bounds))))

(defn log2 [n]
  (/ (Math/log n) (Math/log 2)))

(defn code-length
  "Return -log2(x)"
  [x]
  (- (log2 x)))

(defn bounds-code-length [bounds]
  (code-length (probability-distribution bounds)))

(defn rand-long
  "Converts (rand n) to long and returns it."
  [n]
  (long (rand n)))

(defn random-bound [bounds]
  "Assumes bounds is [lb ub]
  Returns a random value between lb(inclusive) and ub (inclusive)
  Note: rand returns a value between 0(inclusive) to n(exclusive) and hence we add 1."
  (+ (rand-long (+ 1 (duration bounds))) (first bounds)))

(defn if=chosen "Helper function to assert if the given expression if= is the chosen by the planner or not.
i.e Return true if there is a binding for the expression and that the binding is = to the expected."
  [expr bindings]
  (let [condition (first (:value expr))
        expected-value (second (:value expr))
        bound-value (condition bindings)]
    ;(println "if-chosen" condition expected-value bound-value)
    (= bound-value expected-value)))

(defn selector=chosen
  "Helper function to assert if the given bindings have any choice already taken.
  Given nodeid-2-var map, if the value(a set) of any kv-pair has any of the from-vars,
  then any binding for any element in value(a set) implies the choice is bound and that from-var is the
  chosen one.

  Ex: Given
    expr: (selector= :v0-select #{:v2-select :v1-select} :controllable true {:object :node-7, :from-nodes #{:node-7}})
    nodeid-2-var: :node-7 #{:v2 :v2-range :v2-select :v2-reward :v2-cost} -- value
    bindings: {:v0-range 0, :v2-range 0, :v4-range 75, :v5-range 75}

    v2-select will be found in kv for node-7 and v2-range value will be found in bindings, so v2-select
    is the bound value.

    Return all chosen bindings as set. There should only be 1.
    #{:v2-select}
  "
  [expr bindings nid-2-var]
  ;{:post [(<= (count %) 1)]}
  ;(pprint expr)
  (let [choices (filter (fn [kv]
                          ;(println "kv" kv)
                          ;(println "set/intersection" (set/intersection (second kv) (:from-vars expr)))
                          (> (count (set/intersection (second kv) (:from-vars expr))) 0))
                        nid-2-var)
        choice-bindings (into {} (map (fn [kv]
                                        (select-keys bindings (second kv)))
                                      choices))]
    ;(println "Choices" )
    ;(pprint choices)
    ;(println "Choice bindings" choice-bindings)
    choice-bindings))

(defn get-filename-without-suffix [name suffix]
  (first (string/split name suffix)))

(defn get-prefix-of-tpn-json-file [name]
  (first (string/split name #".tpn.json")))

(defn save-bindings [bindings file]
  (save-clj-data {:bindings bindings} file))

(defn var-2-nid
  "Given node -> #{var1 var2}
  Return {var1 node var2 node}"
  [nid2var]
  (reduce (fn [result [node varset]]
            (merge result (reduce (fn [inresult var]
                                    (merge inresult {var node}))
                                  {} varset)))
          {} nid2var))

(defn var-to-node-bindings
  "Given map of nodeid-to-var and var-to-value bindings map,
   return nodeid-to-value map"
  [var-bindings nid2var]
  ;(println "bindings" var-bindings)
  ;(println "initial nid2var")
  ;(pprint nid2var)
  (let [var2nid (var-2-nid nid2var)]
    ;(println "var2nid")
    ;(pprint var2nid)
    (reduce (fn [result [lvar val]]
              (let [node (lvar var2nid)
                    vals-vec (node result)
                    vals-vec (if-not vals-vec [] vals-vec)
                    ]
                (merge result {node (conj vals-vec [lvar val])})))
            {} var-bindings)
    ))

(defn get-binding-value
  "Given a val that can be number? or vector?
  return number or (first val)"
  [val]
  (cond (number? val)
        val
        (vector? val)
        (first val)
        :else
        val))

(defn find-min-bindings [bindings]
  (reduce (fn [result [x val]]
            ;(println result x val)
            (let [x-val (get-binding-value val)]
              (cond (nil? result)
                    x-val
                    :else
                    (min result x-val))))
          nil bindings))

(defn normalize-value [val min-val]
  (cond (number? val)
        (- val min-val)
        (vector? val)
        (into [] (map (fn [v] (- v min-val)) val))
        :else
        val))

(defn normalize-range-bindings
  "Given bindings, normalize range values to begin with 0."
  [bindings]
  (let [range-bindings (filter (fn [binding]
                                 (is-range-var? (first binding))) bindings)
        min-val (find-min-bindings range-bindings)
        normed (reduce (fn [res [k v]]
                         (merge res {k (normalize-value v min-val)})

                         ) {} range-bindings)
        ]
    ;(println "range-bindings")
    ;(pprint range-bindings)
    ;(println "min-val" min-val)
    ;(pprint normed)
    (merge bindings normed)))

(defn bound-and-normalized
  "Assume that values of type number? are real values and of type vector are propagated values.
"
  [bindings]
  (let [real-vals (into {} (filter (fn [[k v]]
                                     (number? v)) bindings))
        normed (normalize-range-bindings real-vals)
        ]
    normed))

(defn to-node-bindings-normalized [bindings nid2var]
  (var-to-node-bindings (bound-and-normalized bindings) nid2var))

(defn choice-paths
  "For a choice begin node,
  return a {:outgoing-null-activity :corresponding-incoming-null-activity}
  for choice-end node" [cnode tpn]
  (into {} (map (fn [act-id]
                  (let [path (find-any-path act-id (:end-node cnode) tpn)]
                    {(first path) (-> path butlast last)}))
                (:activities cnode))))

(defn to-bounds [x]
  "Converts a number to bounds as [x x] and returns it. Otherwise returns x"
  (if (number? x)
    [x x]
    x))

(defn get-max-max-bounds [bounds]
  "Given a list of bounds, ([1 11] [2 12] [3 13]), return bounds [lb ub]
  where lb is maximum of all lbs and ub is maximum of all ubs."
  (reduce (fn [x y]
            (let [xb (to-bounds x)
                  yb (to-bounds y)
                  lb (max (first xb) (first yb))
                  ub (max (second xb) (second yb))]
              [lb ub]))
          bounds))

(defmulti propagate-bounds
          "To propagate bounds through the TPN

          For cost and reward and time,
          :time-bounds are [min max] values upto that object
          :cost-bounds are [min max] values upto that object
          :reward-bounds are [min max] values upto that object
          For choice-end node, min is minimum value across all incoming paths
           and max is maximum across all incoming paths. This assumes only one path is
           chosen at choice begin node

          For parallel-end node, min is sum of all minimum values across all incoming paths
           and max is also sum of all maximum values across all incoming paths.

          return {:uid-of-obj {:bounds propagated-temporal-bounds. They are used for static-uncertainty based learning
                               static-uncertainty optimizes path with least uncertainty.

                               for Overall learning, we need to know what is the range of time, cost, and reward at any given node
                               :time-bounds propagate-time-bounds
                               :cost-bounds propagate-cost-bounds
                               :reward-bounds propagated-reward-bounds

                               :constraint null-or-activity-temporal constraint ; optional field.
                               :cost cost-value ; associated with activity
                               :reward reward-value ; associated with activity
                               :all-bounds for-c-end-or-p-end nodes ; optional field
                               }
                  }"
          (fn [obj from-obj tpn uncertainty]
            (:tpn-type obj)))

(defmethod propagate-bounds :default [obj from-obj tpn unc]
  (to-std-err
    (pprint obj)
    (println "handle propagate-bounds" (:tpn-type obj)))
  (throw (Exception. (str "handle propagate-bounds" (:tpn-type obj)))))

(defmethod propagate-bounds :null-activity [act node _ unc]
  {(:uid act) {:bounds     (add-bounds [0 0] (get-in unc [(:uid node) :bounds]))
               :time-bounds (add-bounds [0 0] (get-in unc [(:uid node) :time-bounds]))
               :cost-bounds (add-bounds [0 0] (get-in unc [(:uid node) :cost-bounds]))
               :reward-bounds (add-bounds [0 0] (get-in unc [(:uid node) :reward-bounds]))
               :constraint [0 0]
               :cost 0 :reward 0
               }})

(defmethod propagate-bounds :activity [act node tpn unc]
  (let [act-value (get-activity-temporal-constraint-value (:uid act) tpn)
        cost-value (if (:cost act)
                     (:cost act)
                     0)
        reward-value (if (:reward act)
                       (:reward act)
                       0)]
    {(:uid act) {:bounds      (add-bounds act-value (get-in unc [(:uid node) :bounds]))
                 :time-bounds (add-bounds act-value (get-in unc [(:uid node) :time-bounds]))
                 :cost-bounds (add-bounds [cost-value cost-value] (get-in unc [(:uid node) :cost-bounds]))
                 :reward-bounds (add-bounds [reward-value reward-value] (get-in unc [(:uid node) :reward-bounds]))
                 :constraint act-value
                 :cost cost-value
                 :reward reward-value}}))

(defmethod propagate-bounds :delay-activity [act node tpn unc]
  (let [act-value (get-activity-temporal-constraint-value (:uid act) tpn)
        cost-value (if (:cost act)
                     (:cost act)
                     0)
        reward-value (if (:reward act)
                       (:reward act)
                       0)]
    {(:uid act) {:bounds      (add-bounds act-value (get-in unc [(:uid node) :bounds]))
                 :time-bounds (add-bounds act-value (get-in unc [(:uid node) :time-bounds]))
                 :cost-bounds (add-bounds [cost-value cost-value] (get-in unc [(:uid node) :cost-bounds]))
                 :reward-bounds (add-bounds [reward-value reward-value] (get-in unc [(:uid node) :reward-bounds]))
                 :constraint act-value
                 :cost cost-value
                 :reward reward-value
                 }}))

(defmethod propagate-bounds :state [node act _ unc]
  (if-not act
    {(:uid node) {:bounds [0 0] :time-bounds [0 0] :cost-bounds [0 0] :reward-bounds [0 0]}}
    {(:uid node) {:bounds (get-in unc [(:uid act) :bounds])
                  :time-bounds (get-in unc [(:uid act) :time-bounds])
                  :cost-bounds (get-in unc [(:uid act) :cost-bounds])
                  :reward-bounds (get-in unc [(:uid act) :reward-bounds])
                  }}))

(defmethod propagate-bounds :c-begin [node act tpn unc]
  (let [paths (choice-paths node tpn)]
    (if-not act
      {(:uid node) {:bounds [0 0] :choice-paths paths
                    :time-bounds [0 0] :cost-bounds [0 0] :reward-bounds [0 0]
                    }}
      {(:uid node) {:bounds       (get-in unc [(:uid act) :bounds])
                    :time-bounds (get-in unc [(:uid act) :time-bounds])
                    :cost-bounds (get-in unc [(:uid act) :cost-bounds])
                    :reward-bounds (get-in unc [(:uid act) :reward-bounds])
                    :choice-paths paths}})))

(defmethod propagate-bounds :c-end [node _ _ unc]
  (let [incoming-bounds (reduce (fn [res act-id]
                                  (conj res {act-id (get-in unc [act-id :bounds])})) {} (:incidence-set node))
        min-of-incoming (apply min-key duration (vals incoming-bounds))
        in-time-bounds (reduce (fn [res act-id]
                                 (conj res {act-id (get-in unc [act-id :time-bounds])})) {} (:incidence-set node))
        in-cost-bounds (reduce (fn [res act-id]
                                 (conj res {act-id (get-in unc [act-id :cost-bounds])})) {} (:incidence-set node))
        in-reward-bounds (reduce (fn [res act-id]
                                   (conj res {act-id (get-in unc [act-id :reward-bounds])})) {} (:incidence-set node))

        min-max-fn (fn [bounds-seq]
                     [(apply min (map first bounds-seq))
                      (apply max (map second bounds-seq))])

        ]
    ;"Propagated bounds for choice node is the one that minimizes uncertainty. i.e lowest duration"
    {(:uid node) {:bounds        min-of-incoming
                  :time-bounds   (min-max-fn (vals in-time-bounds))
                  :cost-bounds   (min-max-fn (vals in-cost-bounds))
                  :reward-bounds (min-max-fn (vals in-reward-bounds))
                  :all-bounds    incoming-bounds}}))

(defmethod propagate-bounds :p-begin [node act _ unc]
  (if-not act
    {(:uid node) {:bounds [0 0] :time-bounds [0 0] :cost-bounds [0 0] :reward-bounds [0 0]}}
    {(:uid node) {:bounds (get-in unc [(:uid act) :bounds])
                  :time-bounds (get-in unc [(:uid act) :time-bounds])
                  :cost-bounds (get-in unc [(:uid act) :cost-bounds])
                  :reward-bounds (get-in unc [(:uid act) :reward-bounds])}}))

(defmethod propagate-bounds :p-end [node _ _ unc]
  ; Propagated bounds for parallel node wants to be [max max] of all incoming edges
  (let [incoming-bounds (reduce (fn [res act-id]
                                  (conj res {act-id (get-in unc [act-id :bounds])})) {} (:incidence-set node))
        in-time-bounds (reduce (fn [res act-id]
                                 (conj res {act-id (get-in unc [act-id :time-bounds])})) {} (:incidence-set node))
        in-cost-bounds (reduce (fn [res act-id]
                                 (conj res {act-id (get-in unc [act-id :cost-bounds])})) {} (:incidence-set node))
        in-reward-bounds (reduce (fn [res act-id]
                                   (conj res {act-id (get-in unc [act-id :reward-bounds])})) {} (:incidence-set node))
        ]
    ;(println "propagate-bounds :p-end")
    ;(pprint in-time-bounds)
    ;(pprint in-cost-bounds)
    ;(pprint in-reward-bounds)
    {(:uid node) {:bounds     (get-max-max-bounds (vals incoming-bounds))
                  :time-bounds     (get-max-max-bounds (vals in-time-bounds))
                  :cost-bounds (apply m-op/+ (vals in-cost-bounds))
                  :reward-bounds (apply m-op/+ (vals in-reward-bounds))
                  :all-bounds incoming-bounds}}))

(defn valid-lb? [lb]
  (not= (or (= lb java.lang.Double/POSITIVE_INFINITY)
            (= lb java.lang.Double/NEGATIVE_INFINITY))))

(defn my-diff [x y]
  "Return x - y according to following rules. Note they are slightly different from java rules.
   Inf - Inf = Inf # Java returns NAN
   Inf - -Inf = Inf
   -Inf - Inf = -Inf
   -Inf - -Inf = -Inf # Java returns NAN"
  (cond (and (= x java.lang.Double/POSITIVE_INFINITY)
             (= y java.lang.Double/POSITIVE_INFINITY))
        java.lang.Double/POSITIVE_INFINITY

        (and (= x java.lang.Double/NEGATIVE_INFINITY)
             (= y java.lang.Double/NEGATIVE_INFINITY))
        java.lang.Double/NEGATIVE_INFINITY

        :else
        (- x y)))

; Note:
;(- java.lang.Double/POSITIVE_INFINITY java.lang.Double/POSITIVE_INFINITY)
;=> NaN
;(- java.lang.Double/POSITIVE_INFINITY java.lang.Double/NEGATIVE_INFINITY)
;=> Infinity
;(- java.lang.Double/NEGATIVE_INFINITY java.lang.Double/POSITIVE_INFINITY)
;=> -Infinity
;(- java.lang.Double/NEGATIVE_INFINITY java.lang.Double/NEGATIVE_INFINITY)
;=> NaN
(defn bounds-diff
  "x and y could be a number or bounds vector. If number then they are converted to bounds as [x x].
   Lower bound of x and y must be a number except java.lang.Double/POSITIVE_INFINITY or java.lang.Double/NEGATIVE_INFINITY
   Upper bound of x and y could be a number or java.lang.Double/POSITIVE_INFINITY or java.lang.Double/NEGATIVE_INFINITY
   returns x - y.
   We treat java constants for +/- Infinity as symbols and handle them as noted:
   Inf - Inf = Inf
   Inf - -Inf = Inf
   -Inf - Inf = -Inf
   -Inf - -Inf = -Inf

   Note: Only following cases will return a bound and nil otherwise
         number - number
         :infinity - ? = :infinity
         number - :infinity = :-infinity"
  [x y]
  (let [xb (to-bounds x)
        yb (to-bounds y)
        xlb (first xb)
        ylb (first yb)
        xub (second xb)
        yub (second yb)]

    (if (not= (valid-lb? xlb))
      (to-std-err (println "Warning: bounds-diff for given x" x " x-lower-bound should not be either +/-Inf. It is " xlb)))

    (if (not= (valid-lb? ylb))
      (to-std-err (println "Warning: bounds-diff for given y" x " y-lower-bound should not be either +/-Inf. It is " ylb)))
    [(my-diff xlb ylb) (my-diff xub yub)]))

(defn choice-path-bounds
  "Given a outgoing choice path and corresponding end path,
  return bounds for the path"
  [cbegin-out cend-in unc]
  (let [b (get-in unc [cend-in :bounds])
        a (get-in unc [cbegin-out :bounds])]
    ;(println "choice-path-bounds b - a " b a cend-in cbegin-out)
    #_[(- (first b) (first a)) (- (second b) (second a))]   ;yields NAN in some cases
    (bounds-diff b a)))

(defn add-path-uncertainty
  "For choice paths, adds :path-uncertainty(bounds) :duration and :weight (probability distribution) "
  [unc node]
  (reduce (fn [res act-id]
            (let [path-uncertainty (choice-path-bounds act-id (get-in res [(:uid node) :choice-paths act-id]) res)
                  res (assoc-in res [act-id :path-uncertainty] path-uncertainty)
                  res (assoc-in res [act-id :duration] (duration path-uncertainty))
                  res (assoc-in res [act-id :weight] (probability-distribution path-uncertainty))
                  ]
              res))
          unc (:activities node)))

(defn update-static-uncertainty
  "For choice begin nodes, adds :path-uncertainty for each outgoing path."
  [unc tpn]
  (reduce (fn [res nid]
            (let [node (get-object nid tpn)]
              (if (= :c-begin (:tpn-type node))
                (add-path-uncertainty res node)
                res)))
          unc (keys unc)))

(defn propagate-time-cost-reward [tpn]
  (reduce (fn [res [uid {from :from}]]
            (conj res (propagate-bounds (get-object uid tpn) (get-object from tpn) tpn res)))
          {} (bfs-walk tpn)))

(defn get-tpn-time-cost-reward [propagated tpn]
  (let [                                                    ;propagated (propagate-time-cost-reward tpn)
        bnode (get-begin-node tpn)
        enode (get-network-end-node tpn bnode)
        tpn-bounds (get propagated (:uid enode))
        ]
    ;(println "get-tpn-time-cost-reward")
    ;(pprint propagated)
    ;(println "end node")
    ;(pprint tpn-bounds)
    (select-keys tpn-bounds [:time-bounds :cost-bounds :reward-bounds])
    ))

(defn propagate-static-uncertainty
  "Propagates bounds through tpn and associates static uncertainty for each choice path"
  [tpn]
  (let [unc-propagated (propagate-time-cost-reward tpn)
        unc-paths-uncertainty (update-static-uncertainty unc-propagated tpn)]
    ;(println "propagate-static-uncertainty unc-propagated" )
    ;(pprint unc-propagated)
    ;(println "unc-paths-uncertainty")
    ;(pprint unc-paths-uncertainty)
    unc-paths-uncertainty))

(defn find-points-around-val
  "val is integer and sd is standard deviation"
  [val sd]
  (let [pts (range 1 (+ 1 sd))
        right-side (map (fn [x]
                          (+ val x)) pts)
        left-side (map (fn [x]
                         (- val x)) pts)
        ]

    {:left-side  left-side
     :right-side right-side}
    ))

(defn make-enum-distribution [pdf]
  ;(println "make-enum-distribution" pdf)
  (new EnumeratedDistribution (map (fn [[key val]]
                                     (new Pair key (double val)))
                                   pdf)))