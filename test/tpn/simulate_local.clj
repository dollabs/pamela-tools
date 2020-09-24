;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

; 10/23/2017.
; Deprecating this namespace as it is not in use.
; WARNING. May not be useful.
(ns tpn.simulate-local
  (:require [clojure.pprint :as pp]
            [clojure.data.json :as json]
            [pamela.tools.dispatcher.tpn-import :as timport]
            [pamela.tools.dispatcher.tpn-walk :as twalk]
            [pamela.tools.dispatcher.tpnrecords :as trecords]
            [pamela.tools.dispatcher.dispatch :as dispatch]
            [pamela.tools.utils.rabbitmq :as broker]
            )
  (:import (java.util Random)))

(def ^:const max-exec-time 5)

(defn act-exec-sim [act objs _]
  (let [sltime (* 1000 (rand-int max-exec-time))]
    (.start (Thread. (fn []
                       (println "activity will become active in" (:uid act) sltime)
                       (Thread/sleep sltime)
                       (println "Start executing " (:uid act) (:name act))
                       (broker/publish-object {:network-id (:network-id objs)
                                               (:uid act)  {:uid              (:uid act)
                                                            :tpn-object-state :active
                                                            }
                                               }
                                              "tpn.activity.active"
                                              nil nil
                                              )
                       )))
    )
  )

(defn act-exec-sim-remote [act objs m]
  "Sleep for a while, then send active message, sleep again and send finished message"
  (act-exec-sim act objs m)
  (let [sltime (* 1000 (+ 2 max-exec-time (rand-int max-exec-time)))]
    (.start (Thread. (fn []
                       (println "activity will finish in" (:uid act) sltime)
                       (Thread/sleep sltime)
                       (println "Stop executing" (:uid act) (:name act))
                       (broker/publish-object {:network-id (:network-id objs)
                                               (:uid act)  {:uid              (:uid act)
                                                            :tpn-object-state :finished
                                                            }
                                               }
                                              "tpn.activity.finished"
                                              nil nil
                                              )

                       )))
    )
  (Thread/sleep 250)
  )

(defn handle-dispatched [dispatched netid objects m]
  (println "Updating state dispatched")
  (pp/pprint dispatched)
  (broker/publish-object (merge dispatched {:network-id netid})
                                   nil nil nil
                                   )
  (Thread/sleep 6000)
  (apply merge (remove nil? (map (fn [[_ v]]
                                   (when (= :negotiation (:tpn-object-state v))
                                     (let [partial-dispatched (dispatch/activity-finished ((:uid v) objects) objects m)]
                                       #_(println "partial-dispatch")
                                       #_(pp/pprint partial-dispatched)

                                       partial-dispatched
                                       ))
                                   ) dispatched))))



; Dispatch network
; Update gui state
; As activities finish, update gui state
; Reset network state
(defn check-dispatch [net-objs]
  #_(pp/pprint net-objs)

  (let [netid (:network-id net-objs)
        network (netid net-objs)
        m {;:activity-dispatcher act-exec-sim                ; Send activity :active message after a 1 sec delay.
           ;:choice-fn tpn.dispatch/first-choice
           }
        net-json (json/write-str net-objs)
        ids (twalk/collect-tpn-ids netid net-objs)
        ]

    #_(clojure.pprint/pprint @state/objects)
    (println "Publishing network")
    (broker/publish-object net-json
                                     nil nil nil
                                     )
    (println "Dispatching netid" netid)

    (loop [to-dispatch (dispatch/dispatch-object network net-objs m)]
      #_(println "to-dispatch")
      (pp/pprint to-dispatch)
      (if (empty? to-dispatch)
        to-dispatch
        (recur (handle-dispatched to-dispatch netid net-objs m))))

    (println "\nFinished?------")
    (clojure.pprint/pprint @dispatch/state)
    #_(dispatch/print-state (twalk/collect-tpn-ids netid net-objs)
                          @dispatch/state
                          net-objs)

    #_(Thread/sleep 3000)
    #_(reset-network ids netid)

    ))

(def remote-config {;:activity-dispatcher act-exec-sim-remote ; Send activity :active message after a 1 sec delay.
                    ;:choice-fn tpn.dispatch/first-choice
                    })

; return a callback fn to be invoked when activity is finished.
(defn check-dispatch-remote [net-objs & [config]]
  #_(pp/pprint net-objs)

  (let [netid (:network-id net-objs)
        network (netid net-objs)
        m remote-config
        ]

    #_(clojure.pprint/pprint @state/objects)
    (println "Publishing network")
    (broker/publish-object net-objs
                                     nil nil nil
                                     )
    (println "Dispatching netid" netid)
    #_(Thread/sleep 2000)
    (let [dispatched (dispatch/dispatch-object network net-objs m)]
      (broker/publish-object (merge dispatched {:network-id netid})))

    ))

(def par-example (trecords/create-parallel-ex))

(def choice-example (trecords/create-choice-ex))

(def sept-14 (timport/from-file "./test/data/sept-demo.json"))

(def par-pamela (timport/from-file "./test/data/parallel.json"))

(def choice-pamela (timport/from-file "./test/data/choice.tpn.json"))

(def par-choice-pamela (timport/from-file "./test/data/parallel-and-choice.tpn.json"))

(defn check-par-dispatch []
  (check-dispatch par-example))

(defn check-choice-dispatch []
  (check-dispatch choice-example))

(defn check-sept-14 []
  (check-dispatch sept-14)
  )

