;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.expr-to-tpn
  (:require [pamela.tools.mct-planner.util :refer :all]
            [pamela.tools.utils.util :as util]
            [pamela.tools.mct-planner.expr :as expr]
            [clojure.string :as string]
            [clojure.pprint :refer :all]))

; To help with co-relating expressions derived from TPN

(defonce tpn-map (atom {}))
(defonce act-counter (atom 0))
(defonce net-counter (atom 0))

(defn make-uid [prefix atom]
  (keyword (str prefix "-" (swap! atom inc))))

(defn filter-range-exprs [exprs]
  (filter (fn [expr]
            (string/ends-with? (:to-var expr) "-range")
            ) exprs))

(defn make-state-node [uid]
  #_(if (uid @tpn-map)
    (do (println "map has" uid)
        (pprint (uid @tpn-map)))
    (println "map does not has" uid))
  (or (uid @tpn-map)
    {:uid           uid
     :tpn-type      :state
     :constraints   #{}
     :activities    #{}
     :incidence-set #{}
     :display-name  uid}))

(defn make-activity [end-node]
  (let [uid (make-uid "act" act-counter)]
    {:uid         uid
     :tpn-type    :activity
     :constraints #{}
     :end-node    end-node
     :args        []
     :argsmap     {}
     :name        uid
     :command     uid}))

(defn make-temporal-constraint [value end-node]
  {:value    value
   :end-node end-node
   :uid      (make-uid "tc" act-counter)
   :tpn-type :temporal-constraint})

(defn wire-activity [from to act]
  ;(println "wire-activity")
  ;(pprint from)
  ;(pprint to)
  ;(pprint act)
  (let [from (update-in from [:activities] (fn [old]
                                             (conj old (:uid act))))
        to (update-in to [:incidence-set] #(conj % (:uid act)))
        act-disp-name (str (:uid from) "->" (:uid to))
        act (assoc act :display-name act-disp-name)
        act (assoc act :name act-disp-name)
        ]
    ;(println "wire-activity" act-disp-name)
    {:to to :from from :act act}))

(defn wire-temporal-constraint [constraint from]
  ;(println "wire temporal constraint" (:value constraint) (:uid from) "->" (:end-node constraint))
  (update-in from [:constraints] #(conj % (:uid constraint))))

(defn collect-begin-nodes [m]
  (remove nil? (map (fn [[k v]]
                      (if (and (contains? v :incidence-set)
                               (= 0 (count (:incidence-set v))))
                        k nil)) m)))

(defn collect-end-nodes [m]
  (remove nil? (map (fn [[k v]]
                      (if (and (contains? v :activities)
                               (= 0 (count (:activities v))))
                        k nil)) m)))

(defn make-network [m]
  (let [begins (collect-begin-nodes m)
        ends (collect-end-nodes m)
        uid (make-uid "net" net-counter)]
    (println "make-network begins" begins)
    (println "make-network ends" ends)
    {:network-id uid
     uid      {
                  :tpn-type   :network
                  :uid        uid
                  :begin-node (first begins)
                  :end-node   (first ends)
                  }}))

(defmulti make-tpn-object
          "Create TPN objects for each expression type"
          (fn [expr]
            (:symbol expr)))

(defmethod make-tpn-object :default [expr]
  (util/to-std-err (println "Impl make-tpn-object" (:symbol expr) expr)
              ))

(defmethod make-tpn-object '= [expr]
  ;(println "=" expr)
  (let [from (make-state-node (first (:from-vars expr)))
        to (make-state-node (:to-var expr))
        act (make-activity (:uid to))
        {to :to from :from act :act} (wire-activity from to act)
        ]
    ;(pprint from)
    ;(pprint to)
    ;(pprint act)
    (swap! tpn-map assoc (:uid from) from)
    (swap! tpn-map assoc (:uid to) to)
    (swap! tpn-map assoc (:uid act) act)))

(defmethod make-tpn-object 'if= [expr]
  ;(println "if=" expr)
  (let [from (make-state-node (first (:from-vars expr)))
        to (make-state-node (:to-var expr))
        act (make-activity (:uid to))
        {to :to from :from act :act} (wire-activity from to act)
        ]
    ;(pprint from)
    ;(pprint to)
    ;(pprint act)
    (swap! tpn-map assoc (:uid from) from)
    (swap! tpn-map assoc (:uid to) to)
    (swap! tpn-map assoc (:uid act) act)))

(defmethod make-tpn-object 'in-range [expr]
  ;(println "in-range" expr)
  (let [from (make-state-node (first (:from-vars expr)))
        to (make-state-node (:to-var expr))
        act (make-activity (:uid to))
        {to :to from :from act :act} (wire-activity from to act)
        tc (make-temporal-constraint (:value expr) (:uid to))
        from (wire-temporal-constraint tc from)
        ]
    ;(pprint from)
    ;(pprint to)
    ;(pprint tc)
    (swap! tpn-map assoc (:uid from) from)
    (swap! tpn-map assoc (:uid to) to)
    (swap! tpn-map assoc (:uid act) act)
    (swap! tpn-map assoc (:uid tc) tc)
    ))

(defmethod make-tpn-object 'in-range-max [expr]
  (doseq [from-id (:from-vars expr)]
    (let [from (make-state-node from-id)
          to (make-state-node (:to-var expr))
          act (make-activity (:uid to))
          {to :to from :from act :act} (wire-activity from to act)
          ]

      ;(pprint from)
      ;(pprint to)
      ;(pprint act)
      (swap! tpn-map assoc (:uid from) from)
      (swap! tpn-map assoc (:uid to) to)
      (swap! tpn-map assoc (:uid act) act))))

(defn make-expr-tpn-helper [exprs out-file]
  (reset! act-counter 0)
  (reset! tpn-map {})
  ; Side effecting here to create objects and update tpn-map
  (doseq [expr exprs]
    (make-tpn-object expr))
  (swap! tpn-map merge (make-network @tpn-map))
  ;(println "begin" (collect-begin-nodes @tpn-map))
  ;(println "network" (make-network @tpn-map))
  ;(json/to-file @tpn-map out-file)
  (save-clj-data @tpn-map out-file)
  (doseq [expr exprs]
    (pprint expr))
  )

(defn make-expr-tpn [from-file]
  (reset! net-counter 0)
  (let [{all :all-exprs exprs :exprs} (expr/make-expressions-from-file from-file)
        all (filter-range-exprs all)
        exprs (filter-range-exprs exprs)
        file-prefix (get-filename-without-suffix from-file #"\.tpn.json")
        ]
    (println "Creating expr-tpn for" from-file)
  (make-expr-tpn-helper all (str file-prefix
                                 ".all-range.tpn.edn"))
  (make-expr-tpn-helper exprs (str file-prefix
                                 ".some-range.tpn.edn"))))