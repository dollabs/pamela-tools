;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.utils.tpn-types
  "In absence of formal type system, need to store hierarchy information")

(def nodetypes #{:state :p-begin :p-end :c-begin :c-end})
(def edgetypes #{:null-activity :activity :delay-activity})

(def begin-nodes #{:p-begin :c-begin})
(def end-nodes #{:p-end :c-end})

(def tpn-to-symbol {:cost         'cost= :reward 'reward= :temporal-constraint 'in-range :null-activity '=
                    :in-range-max 'in-range-max})

; When choosing var of which node to use in constraints, we prefer begin nodes then end nodes and then state
(def equivalent-class-preferences {:p-begin 0 :p-end 2
                                   :c-begin 1 :c-end 3
                                   :state   4})
