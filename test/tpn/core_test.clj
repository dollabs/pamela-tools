;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns tpn.core-test
  (:require [tpn.test-support :refer :all]

            [pamela.tools.dispatcher.dispatch-app :as dapp]
            [pamela.tools.dispatcher.tpnrecords :as trecords]
            [pamela.tools.dispatcher.tpn-walk :as twalk]
            [pamela.tools.dispatcher.tpn-import :as timport]
            [pamela.tools.utils.tpn-json :as tpn_json]
            [pamela.tools.utils.rabbitmq :as rmq]
            [pamela.tools.plant.interface :as plant]
            [pamela.tools.plant.plant-sim :as psim]
            [me.raynes.fs :as fs]
            [clojure.test :refer :all]
            [clojure.data.json :as json]
            [clojure.pprint :refer :all]
            [clojure.data]
            [pamela.tools.mct-planner.planner :as planner]))

#_(defn show-simple-parallel []
    (tpn.util/send-to "localhost" 34170 (slurp "test/data/accord/simple-parallel.json")))

(deftest create-example
  (testing "Create TPN from code"
    (let [objects (trecords/create-parallel-ex)
          network ((:network-id objects) objects)
          bnode ((:begin-node network) objects)
          enode ((:end-node bnode) objects)
          pb-null ((first (:activities bnode)) objects)
          sb ((:end-node pb-null) objects)
          act ((first (:activities sb)) objects)
          tcontraint ((first (:constraints act)) objects)
          to-json-net (twalk/collect-tpn-with-netid (:uid network) objects)
          as-json-str (json/write-str to-json-net)
          to-map (tpn_json/map-from-json-str as-json-str)
          ]
      #_(println "p-begin")
      #_(clojure.pprint/pprint bnode)

      #_(println "pb-null")
      #_(clojure.pprint/pprint pb-null)

      #_(println "state begin")
      #_(clojure.pprint/pprint sb)

      #_(println "activity")
      #_(clojure.pprint/pprint act)

      #_(println "p-end")
      #_(println enode)

      ;(clojure.pprint/pprint  to-json-net)
      (is (not= nil network))
      (is (not= nil bnode))
      (is (not= nil enode))
      (is (not= nil pb-null))
      (is (not= nil sb))
      (is (not= nil act))
      (is (not= nil (:end-node act)))
      (is (not= nil tcontraint))

      (check-begin bnode pamela.tools.dispatcher.tpnrecords.p-begin pamela.tools.dispatcher.tpnrecords.p-end objects)
      (check-end enode objects)
      (is (= (:name act) "Background Activities"))
      (is (= (first (:value tcontraint)) 160))
      (is (= (second (:value tcontraint)) 170))
      ;(pprint objects)
      ;(pprint to-json-net)
      (is (= (count objects) (count to-json-net)))
      (= to-map to-json-net)
      #_(tpn.fromjson/to-file to-json-net "./test/data/create-parallel-ex.json"))
    #_(tpn.fromjson/from-file "./test/data/create-parallel-ex.json")
    ))

; TPN code is now created by pamela.
#_(deftest example-tpn
  (testing "TPN To and From file"
    (let [objects (trecords/create-parallel-ex)
          net ((:network-id objects) objects)
          tpn (twalk/collect-tpn-with-netid (:uid net) objects)
          _ (tpn_json/to-file tpn "./test/data/create-parallel-ex.test.json")
          tpn-new (timport/from-file "./test/data/create-parallel-ex.test.json")
          ;[in-to in-from in-both] (clojure.data/diff tpn tpn-new)
          ]
      (is (= tpn tpn-new))
      ;(println "only in to file")
      ;(pprint in-to)
      ;(println "only in from file")
      ;(pprint in-from)
      )))

(defn check-collect-tpn []
  (let [objects (trecords/create-parallel-ex)
        net ((:network-id objects) objects)
        tpn (twalk/collect-tpn-with-netid (:uid net) objects)]
    #_(json/pprint tpn)
    (tpn_json/to-file tpn "./test/data/create-parallel-ex.json")
    ))

(defn check-transient []
  (let [m (transient {})]
    (assoc! m :1 1 :2 2 :3 3 :4 4 :5 5 :6 6 :7 7 :8 8 :9 9 :10 10)
    (persistent! m)))

(defn checl-local-vars []
  (with-local-vars [lvar #{}
                    ]
    (doseq [x #{:a :b}]
      (var-set lvar (conj @lvar x))
      )
    @lvar)
  )

(defn check-return [l]
  (if (empty? l)
    {}
    (conj {(first l) (first l)} (check-return (rest l)))))

(defn dispatch-tpn [args-v]
  (dapp/go args-v))

(defn start-plant-sim []
  (psim/-main "--plant-ids" ""))

(defn stop-plant-sim []
  (psim/stop-plant-processing))

(def test-tpns [["test/repr/data/choice/choice.feasible.cfm.tpn.json"]
                ["test/repr/data/dedpac-apr-2017/dedpac-apr-2017.tpn.json"]
                ["test/repr/data/edgect-june-2016/edgect-june-2016.tpn.json"]
                ["test/repr/data/feasible/feasible.tpn.json"]
                ["test/repr/data/first/first.tpn.json"]
                ["test/repr/data/sequence.feasible/sequence.feasible.tpn.json"]
                ["test/repr/data/sequence.infeasible/sequence.infeasible.tpn.json"]])

; Generated using (tpn.test-support/get-all-tpn-files-as-vec)
(def all-tpns [
               ;["test/data/choice.json"]
               ;["test/data/create-example.test.json"]
               ;["test/data/create-parallel-ex.test.json"]
               ;["test/data/parallel-and-choice.json"]
               ;["test/data/parallel.json"]
               ;["test/data/sept-demo.json"]
               ;["test/data/two-network-flows.json"]
               ["test/repr/data/bindings/ato-20171116-manually-expanded.tpn.json"]
               ["test/repr/data/bindings/simple-choice.tpn.json"] ; FIXME Does not finish with learning
               ["test/repr/data/bindings/simple-parallel.tpn.json"]
               ["test/repr/data/choice/choice-1-cost-controllable.tpn.json"] ; FIXME Does not finish
               ["test/repr/data/choice/choice-1-cost.tpn.json"]
               ["test/repr/data/choice/choice-1-reward-controllable.tpn.json"]
               ["test/repr/data/choice/choice-1-reward.tpn.json"]
               ["test/repr/data/choice/choice.feasible.cfm.tpn.json"]
               ["test/repr/data/dedpac-apr-2017/dedpac-apr-2017.tpn.json"]
               ["test/repr/data/edgect-june-2016/edgect-june-2016.all-range.tpn.json"]
               ["test/repr/data/edgect-june-2016/edgect-june-2016.some-range.tpn.json"]
               ["test/repr/data/edgect-june-2016/edgect-june-2016.tpn.json"]
               ["test/repr/data/feasible/feasible.tpn.json"]
               ["test/repr/data/first/first.cfm.tpn.json"]
               ["test/repr/data/first/first.tpn.json"]
               ["test/repr/data/paper/choice-1-cost.tpn.json"]
               ["test/repr/data/paper/choice-1-learn-optimize-high-reward.tpn.json"]
               ["test/repr/data/paper/choice-1-learn-optimize-low-bounds.tpn.json"]
               ["test/repr/data/paper/choice-1-learn-optimize-low-cost.tpn.json"]
               ["test/repr/data/paper/choice-1-reward.tpn.json"]
               ["test/repr/data/paper/choice-1.tpn.json"]
               ["test/repr/data/paper/choice-5.tpn.json"]
               ["test/repr/data/sequence.feasible/infinite-sequence.tpn.json"]
               ["test/repr/data/sequence.feasible/sequence.feasible.tpn.json"]
               ["test/repr/data/sequence.infeasible/sequence.infeasible.tpn.json"]
               ["test/repr/data/uncertainty/ato-small.tpn.json"]
               ["test/repr/data/uncertainty/seq-of-choices.tpn.json"]])

(defn test-dispatch-tpn-and-wait
  "Dispatches TPN and waits until finished or stopped because of error.
  Time out if it takes too long"
  [tpn]
  ; Assuming last argument of 'tpn' is filename
  ; Assuming all tpn file names are unique
  (def tpn_timeout_secs 20)
  (let [tpn-outf (str (last tpn) ".out")
        tpn-errf (str (last tpn) ".err")]
    (println "\n-------------------------------")
    ;(println "Sending output to: " tpn-outf)
    (println "Starting TPN:" tpn)

    #_(with-open [out (clojure.java.io/writer tpn-outf)
                err (clojure.java.io/writer tpn-errf)]

      #_(binding [*out* out
                *err* err]
        (dispatch-tpn tpn)))
    (dispatch-tpn tpn)
    (let [finish-reason (dapp/wait-until-tpn-finished #_(+ (* tpn_timeout_secs 1000) (System/currentTimeMillis)))]
      (println "TPN Finish reason" tpn "\n" finish-reason)
      ; TODO FIXME
      #_(is (true? (not (:stop-tpn-processing finish-reason))) (str tpn-outf " :TPN dispatch stopped due to exceptions"))
      #_(is (true? (not (:timed-out finish-reason))) (str tpn-outf " :TPN dispatch stopped due to timeout"))
      )

    (println "Finished TPN testing:" tpn)

    ))

(defn dispatch-tpns [tpns]
  (println "Dispatching tpns\n" tpns "\n")
  (doseq [tpn tpns #_(take 2 tpns)]

    (test-dispatch-tpn-and-wait tpn)
    (Thread/sleep 2000)

    ))
; (run-tests 'tpn.core-test)

(defn test-all-tpns []
  (let [tpns all-tpns #_(get-all-tpn-files-in-test)]

    ; start plant-sim to listen for messages on all plant-ids
    ;(start-plant-sim)
    ; dispatch tpns
    (dispatch-tpns tpns)

    ; wait for each tpn to finish. Catch errors / exceptions?

    ; stop plant-sim
    ;(stop-plant-sim)
    )
  #_(testing "Testing all TPNs dispatch and finish"

    ))


(defn send-observation-message
  "Function to send object state as observation message"
  [net-id act-id object-state]
  (plant/observations (dapp/get-plant-interface)
                      "planviz"
                      nil
                      [{:field :tpn-object-state
                        :value {:network-id net-id
                                act-id {:uid act-id
                                        :tpn-object-state object-state}}}]
                      nil))

(defn send-do-not-wait
  "Fn to send a do not wait message for an activity.
  "
  [& [net-id act-id]]
  (send-observation-message (or net-id :net-3) (or act-id :act-14) :do-not-wait)
  #_(let [net-id (or net-id :net-3)
        act-id (or act-id :act-14)
        a1 {:network-id net-id
            act-id        {:uid act-id :tpn-object-state :do-not-wait}}
        exch (tpn.dispatch-app/get-exchange-name)
        plant (tpn.dispatch-app/get-plant-interface)]
    (plant/observations plant "planviz" nil [{:field :tpn-object-state
                                              :value a1
                                              }] nil)))

(defn send-cancel-activity
  "Fn to send a cacnel-activity message.
  "
  [& [net-id act-id]]
  (send-observation-message (or net-id :net-3) (or act-id :act-14) :cancel-activity)
  #_(let [net-id (or net-id :net-3)
        act-id (or act-id :act-14)
        a1 {:network-id net-id
            act-id        {:uid act-id :tpn-object-state :do-not-wait}}
        exch (tpn.dispatch-app/get-exchange-name)
        plant (tpn.dispatch-app/get-plant-interface)]
    (plant/observations plant "planviz" nil [{:field :tpn-object-state
                                              :value a1
                                              }] nil)))

(defn fail-tpn-cb [tpn node-state]
  (println "TPN failed. replanning")
  ;(pprint tpn)
  (println "node state")
  (pprint node-state)
  (def node-state node-state)
  ;(def new-expr-bindings (planner/temporal-bindings-from-tpn-state (:nid-2-var exprs) node-state))
  ; solve again and see what comes
  #_(let [again (planner/solve tpn new-expr-bindings 5)]
    (if (nil? (:bindings again))
      (println "Planner failed to find solution for given node state and 5 iterations"))))

(defn test-failed-tpn [file & [bindings]]
  (dapp/set-tpn-failed-handler fail-tpn-cb)
  (dispatch-tpn [file])

  (def tpn (dapp/get-tpn))
  (def exprs (planner/solve tpn bindings)))

(defn read-all-tpns []
  (reduce (fn [res tpn]
            (conj res (tpn_json/from-file tpn))
            ) [] all-tpns))
; Manual testing
; - With plant sim, all TPNs should dispatch and finish as expected. Some temporal constraints will fail because of
; timing issues but there should not be any exceptions/errors
; - With plant sim in classic mode, feasible TPNs should finish without any temporal constraint violations.
; - With plant sim in fail mode, one or more activities will fail and dispatcher should exit after all activities have finished
