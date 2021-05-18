;
; Copyright © 2020 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns mct-planner.planner-test
  (:require                                                 ;[clojure.test :refer :all]
    [pamela.tools.mct-planner.planner :as planner]
    [pamela.tools.dispatcher.tpn-import :as tpn_import]
    [pamela.tools.dispatcher.dispatch-app :as dapp]
    [clojure.pprint :refer :all]
    [tpn.core-test]))


(defn from-file [name]
  (planner/solve (tpn_import/from-file name) nil))

(defn plan-all
  "Trivial regression test to ensure planner does not crash when solving for known
  TPNs"
  []
  (doseq [tpn tpn.core-test/all-tpns #_(take 1 tpn.core-test/all-tpns)]
    (println (first tpn))
    (from-file (first tpn))))

(defn plan-and-dispatch [fname]
  (let [pdata (from-file fname)
        delayed-dispatch (* 5 1000)]

    (def planned pdata)
    ; Ensure dapp name space is (dapp/go) with proper arguments before calling this function
    (dapp/go ["--force-plant-id"])
    (dapp/setup-and-dispatch-tpn-with-bindings (:tpn pdata) (:bindings pdata)
                                               (+ delayed-dispatch (System/currentTimeMillis)))
    (println "Node bindings")
    (pprint (:bindings pdata))
    (dapp/wait-until-tpn-finished)))


(defn plan-and-dispatch-all []
  (doseq [[tpn] tpn.core-test/all-tpns]
    (println "Testing with planner bindings" tpn)
    (plan-and-dispatch tpn)))

; Invoke as
; (run-planner "test/repr/data/sequence.feasible/sequence.feasible.tpn.json" {:node-4 0
;                                                                            :node-1 4})
(defn run-planner [tpn-file bindings]
  (let [tpn (tpn_import/from-file tpn-file)
        {tpn :tpn bindings :bindings} (planner/solve-with-node-bindings tpn bindings 1)]
    (dapp/set-node-bindings bindings)
    (dapp/setup-and-dispatch-tpn tpn)
    bindings))





; FIXME
#_(defn test-bindings-to-json []
    (planner/infinity-to-string {:node-14 {:temporal-value [0 Infinity]},
                                 :node-6  {:temporal-value 0},
                                 :node-1  {:temporal-value [0 Infinity]},
                                 :node-10 {:temporal-value [0 Infinity]}}))
