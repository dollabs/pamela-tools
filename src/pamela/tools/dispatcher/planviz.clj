;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.dispatcher.planviz
  (:require [pamela.tools.utils.rabbitmq :as rmq]))


(defprotocol planvizI
  "An interface to send messages to planviz"
  (reset-network [rmq tpn-map]
    "Request planviz to reset state")
  (impossible [rmq obj-ids network-id]
    "Let planviz know that given objects are in impossible state")
  (failed [rmq obj-ids network-id]
    "Let planviz know that given objects are in failed state")
  (normal [rmq obj-ids network-id]
    "Let planviz know that given objects are in normal state")
  (cancel [rmq obj-ids network-id]
    "Let planviz know that given objects are in cancel state")
  (cancelled [rmq obj-ids network-id]
    "Let planviz know that given objects are in cancelled state"))


(defn make-object-update-map [obj-ids state network-id]
  ;(println obj-ids state network-id)
  (let [up-m (reduce (fn [result id]
                       ;(println "returning " {id {:uid id :tpn-object-state state}})
                       (conj result {id {:uid id :tpn-object-state state}}))
                     {} obj-ids)]
    (merge {:network-id network-id} up-m)))

(defn publish-object-state [state obj-ids network-id channel exchange]
  (rmq/publish-object (make-object-update-map obj-ids state network-id)
                      "tpn.object.update"
                      channel
                      exchange))

; channel is rmq object
; exchange is rmq exchange name of type string
(defrecord rmq [channel exchange] :load-ns true
  planvizI
  (reset-network [this tpn-map]
    (println "planviz impl: reset-network"))

  (impossible [this obj-ids network-id]
    ;(println "planviz impl: impossible")
    (publish-object-state :impossible obj-ids network-id (:channel this) (:exchange this))
    )
  (failed [this obj-ids network-id]
    ;(println "planviz impl: failed")
    (publish-object-state :failed obj-ids network-id (:channel this) (:exchange this))
    )
  (normal [this obj-ids network-id]
    ;(println "planviz impl: failed")
    (publish-object-state :normal obj-ids network-id (:channel this) (:exchange this))
    )
  (cancel [this obj-ids network-id]
    ;(println "planviz impl: failed")
    (publish-object-state :cancel obj-ids network-id (:channel this) (:exchange this))
    )
  (cancelled [this obj-ids network-id]
    ;(println "planviz impl: failed")
    (publish-object-state :cancelled obj-ids network-id (:channel this) (:exchange this)))
  )

;;
(defn make-rmq
  [channel exchange]
  (->rmq channel exchange))

; Example message format
#_{:node-38    {:uid :node-38, :tpn-object-state :reached},
   :act-39     {:uid :act-39, :tpn-object-state :finished},
   :node-4     {:uid :node-4, :tpn-object-state :reached},
   :act-41     {:uid :act-41, :tpn-object-state :finished},
   :node-26    {:uid :node-26, :tpn-object-state :reached},
   :act-29     {:uid :act-29, :tpn-object-state :finished},
   :node-20    {:uid :node-20, :tpn-object-state :reached},
   :network-id :net-43}