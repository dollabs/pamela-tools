;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.sample-i)

; Data about finding a sample.

;   We create exprs for all choice paths, however at runtime, we chose only 1 path
;   so  only the expr which has to-var bound will expr-value
(defrecord sample [exprs bindings expr-values var-code-length satisfies]
  ; exprs is a list

  ; bindings is map of vars to values; No value or nil value implies vars are not bound

  ; expr-values is map of expr to values. We choose value for controllable only. Rest are simply propagated and hence no value

  ; var-code-length is a map of to-var as key and  value is code length of the var
  ; at any given point in time. Incoming expressions contribute to the code-length of the var.

  ; satisfies is map of exprs and true/false where true implies bindings satisfies expression
  )
(defn hello-sample [])
;; defrecords are better of being in a separate name space otherwise there are weird issues when working in repl.