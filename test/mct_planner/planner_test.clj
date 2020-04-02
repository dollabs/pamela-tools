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
    [tpn.core-test]
    ))

(defn from-file [name]
  (planner/solve (tpn_import/from-file name) nil))

(defn do-all
      "Trivial regression test to ensure planner does not crash when solving for known
      TPNs"
      []
  (doseq [tpn tpn.core-test/all-tpns #_(take 1 tpn.core-test/all-tpns) ]
    (println (first tpn))
    (from-file (first tpn))))