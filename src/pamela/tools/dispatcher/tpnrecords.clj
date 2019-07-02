;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.dispatcher.tpnrecords
  (:require [pamela.tools.utils.util :as util]))

; Structures to hold some information
(defrecord network [uid begin-node])                        ; Network contains begin node of TPN

; Nodes
(defrecord state [uid activities constraints incidence-set])
(defrecord p-begin [uid activities constraints incidence-set end-node])
(defrecord p-end [uid activities incidence-set])
(defrecord c-begin [uid activities constraints incidence-set end-node])
(defrecord c-end [uid activities incidence-set])

; Arcs
(defrecord null-activity [uid end-node])                    ; constraints will have single object containing temporal constraints [0 0]
(defrecord activity [uid name cost reward constraints end-node])
(defrecord delay-activity [uid name constraints end-node])

; Constraints
(defrecord temporal-constraint [uid value end-node])

; Constructor fns. Maybe a macro to generate them would be nice
; ------------------------------------------------------------------------------------------------------------------
(defn make-state [m & {:keys [uid activities constraints incidence-set]
                       :or   {uid         (keyword (gensym "node-")), activities #{},
                              constraints #{}, incidence-set #{}}}]
  "Node constructor. Create with supplied values or defaults and then merge with given map"
  (merge (->state uid activities constraints incidence-set) m {:tpn-type :state}))

(defn make-p-begin [m & {:keys [uid activities constraints incidence-set end-node]
                         :or   {uid         (keyword (gensym "node-")), activities #{},
                                constraints #{}, incidence-set #{}, end-node nil}}]
  (merge (->p-begin uid activities constraints incidence-set end-node) m {:tpn-type :p-begin}))

(defn make-p-end [m & {:keys [uid activities incidence-set]
                       :or   {uid (keyword (gensym "node-")), activities #{}, incidence-set #{}}}]
  (merge (->p-end uid activities incidence-set) m {:tpn-type :p-end}))

(defn make-c-begin [m & {:keys [uid activities constraints incidence-set end-node]
                         :or   {uid         (keyword (gensym "node-")), activities #{},
                                constraints #{}, incidence-set #{}, end-node nil}}]
  (merge (->c-begin uid activities constraints incidence-set end-node) m {:tpn-type :c-begin}))

(defn make-c-end [m & {:keys [uid activities incidence-set]
                       :or   {uid (keyword (gensym "node-")), activities #{}, incidence-set #{}}}]
  (merge (->c-end uid activities incidence-set) m {:tpn-type :c-end}))

(defn make-activity [m & {:keys [uid name cost reward constraints end-node]
                          :or   {uid (keyword (gensym "act-")), name "", cost 0, reward 0, constraints #{}, end-node nil}}]
  "Activity constructor. Create with supplied values or defaults and then merge with given map"
  (merge (->activity uid name cost reward constraints end-node) m {:tpn-type :activity}))

(defn make-null-activity [m & {:keys [uid end-node]
                               :or   {uid (keyword (gensym "act-"))}}]
  "Null activity constructor."
  (merge (->null-activity uid end-node) m {:tpn-type :null-activity}))

(defn make-delay-activity [m & {:keys [uid name constraints end-node]
                                :or   {uid (keyword (gensym "delay-")), name "Wait", constraints #{}, end-node nil}}]
  "Delay activity constructor"
  (merge (->delay-activity uid name constraints end-node) m {:tpn-type :delay-activity}))

; Since network is not exactly a TPN object but an enclosing container, we do not add :tpn-type to it.
(defn make-network [m & {:keys [uid begin-node]
                         :or   {uid (keyword (gensym "net-"))}}]
  "Network constructor. Create with supplied values or defaults and then merge with given map"
  (if-not uid
    (merge (->network (keyword (gensym "net-")) begin-node) m {:tpn-type :network})
    (merge (->network uid begin-node) m {:tpn-type :network})))

(defn make-temporal-constraint [m & {:keys [uid value end-node]
                                     :or   {uid (keyword (gensym "cnstr-")) value [0 'infinity]}}]
  (merge (->temporal-constraint uid value end-node) m {:tpn-type :temporal-constraint}))

; :tpn-type to constructor-fn map
(def tpn-type-2-create {:state          make-state :p-begin make-p-begin :p-end make-p-end
                        :c-begin        make-c-begin :c-end make-c-end :null-activity make-null-activity
                        :activity       make-activity :temporal-constraint make-temporal-constraint :network make-network
                        :delay-activity make-delay-activity})

; Rest
(defn wire-activity [from act to]
  "Updates from node's activity set and to node's incidence set with the given activity."
  [(update from :activities conj (:uid act))
   (update to :incidence-set conj (:uid act))
   ])

(defn get-begin-node [netid objs]
  (if-not (netid objs)
    (util/debug-object (str "netid does not exists " netid) nil get-begin-node)
    ((:begin-node (netid objs)) objs)))

; (parallel
;   (plant$background-activities :bounds [160 170]))
(defn create-parallel-ex []
  (with-local-vars [pe (make-p-end {})
                    pb (make-p-begin {})
                    sb (make-state {})
                    se (make-state {})
                    ]
    (var-set pb (merge @pb {:end-node (:uid @pe)}))

    (let [constraint (make-temporal-constraint {} :value [160 170])
          act (make-activity {} :constraints #{(:uid constraint)}
                             :name "Background Activities"
                             :end-node (:uid @se))
          pb-null (make-null-activity {} :end-node (:uid @sb))
          pe-null (make-null-activity {} :end-node (:uid @pe))
          network (make-network {} :begin-node (:uid @pb))
          pb-null-wired (wire-activity @pb pb-null @sb)
          ;_ (println "pb-null-wired" )
          ;_ (clojure.pprint/pprint pb-null-wired)
          _ (var-set pb (first pb-null-wired))
          _ (var-set sb (second pb-null-wired))
          act-wired (wire-activity @sb act @se)
          _ (var-set sb (first act-wired))
          _ (var-set se (second act-wired))
          pe-null-wired (wire-activity @se pe-null @pe)
          _ (var-set se (first pe-null-wired))
          _ (var-set pe (second pe-null-wired))
          ]
      {(:uid @pb)        @pb
       (:uid @pe)        @pe
       (:uid @sb)        @sb
       (:uid @se)        @se
       (:uid act)        act
       (:uid constraint) constraint
       (:uid pb-null)    pb-null
       (:uid pe-null)    pe-null
       (:uid network)    network
       :network-id       (:uid network)}
      )))

(defn create-choice-ex []
  (with-local-vars [ce (make-c-end {})
                    cb (make-c-begin {})
                    sb (make-state {})
                    se (make-state {})
                    ]
    (var-set cb (merge @cb {:end-node (:uid @ce)}))

    (let [constraint (make-temporal-constraint {} :value [160 170])
          act (make-activity {} :constraints #{(:uid constraint)}
                             :name "Background Activities"
                             :end-node (:uid @se))
          pb-null (make-null-activity {} :end-node (:uid @sb))
          pe-null (make-null-activity {} :end-node (:uid @ce))
          network (make-network {} :begin-node (:uid @cb))
          pb-null-wired (wire-activity @cb pb-null @sb)
          ;_ (println "cb-null-wired" )
          ;_ (clojure.pprint/pprint cb-null-wired)
          _ (var-set cb (first pb-null-wired))
          _ (var-set sb (second pb-null-wired))
          act-wired (wire-activity @sb act @se)
          _ (var-set sb (first act-wired))
          _ (var-set se (second act-wired))
          pe-null-wired (wire-activity @se pe-null @ce)
          _ (var-set se (first pe-null-wired))
          _ (var-set ce (second pe-null-wired))
          ]
      {(:uid @cb)        @cb
       (:uid @ce)        @ce
       (:uid @sb)        @sb
       (:uid @se)        @se
       (:uid act)        act
       (:uid constraint) constraint
       (:uid pb-null)    pb-null
       (:uid pe-null)    pe-null
       (:uid network)    network
       :network-id       (:uid network)}
      )))