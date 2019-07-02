;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.core
  (:require [pamela.tools.mct-planner.expr :as expr]
            [pamela.tools.mct-planner.solver :refer :all]
            [pamela.tools.mct-planner.expr-to-tpn]
            [pamela.tools.mct-planner.util]))

#_(defn solve-tpn [fromfile]
  (let [exprs (constraints-from-tpn fromfile)]
    (solve exprs 1)
    #_(solve (:constraints exprs-data)
             (:final-vars exprs-data)                       ; Set of all vars in system of constraints
             {(:begin-var exprs-data) 0}                    ;Initial binding. Could be {}
             ;{}                                               ;Need initial binding for the algorithm to terminate
             ; TODO handle the case when initial binding is empty.
             20)

    #_(repr.solver-two/solve (:constraints exprs-data)
                             (:final-vars exprs-data)       ; Set of all vars in system of constraints
                             {(:begin-var exprs-data) 0}    ;Initial binding. Could be {}
                             2)
    #_(solve-old (:constraints exprs-data)
                 (:final-vars exprs-data)                   ; Set of all vars in system of constraints
                 {(:begin-var exprs-data) 0}                ;Initial binding. Could be {}
                 20)))

(defn generate-artifacts [tpn-json-file]
  (let [prefix  (pamela.tools.mct-planner.util/get-prefix-of-tpn-json-file tpn-json-file)]
    (println "Creating expression info" tpn-json-file "\n" prefix "\n" (str prefix ".edn"))
    (expr/save-tpn-expression-info tpn-json-file (str prefix ".edn"))
    (println "Creating expr tpn for" tpn-json-file)
    (pamela.tools.mct-planner.expr-to-tpn/make-expr-tpn tpn-json-file)
    )
  )

(comment
  -- use iterations and collect results
  -- Test with first.json
  -- Update max etc constraints.
  -- What about vars with * as value)


; 1. track constraint violation. Which constraints failed for each sample.
;    For each value of var, which expression failed.
; once we know that a constraint expression failed or success, we should not check again.
; i.e we check only if they have unbound vars.

; 2. entropy calculation to select next variable

; 3. add cost and reward vars.

; 4. Need measurement of how good a sample is.

; 5. add choice constraint




