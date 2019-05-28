;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant-examples.command)

(defprotocol commandI
  "Interface to support exec method implementation. See exec-plant.pamela"
  (id [obj]
    "Return of the object")
  (command [obj]
    "Return full command with args")
  (get-state [obj]
    "Return current state of the object")
  (update-state! [obj in-state]
    "return object with its state changed as given")

  (start-command [obj]
    "return the object with state changed as appropriate with any other information
    that might help future operations on the object.
    If the object has successfully started, it should change its state to :started and then return itself")
  (cancel-command [obj])
  (get-status [obj]
    "Return current status of the command as {} or
    {:completion-time a-time-in-future, :percent-complete 42}
    If available, completion-time is required but percent-complete is optional.
    ")
  )