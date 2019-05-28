;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.plant.interface)

;Rabbit MQ interface. Convenience methods for clojure implementation of plant.
; Other languages may define their own interface.
(defprotocol plant-rmq
  (setup-connection [plant-connection]                      ;
    "Assume plant-connection is a defrecord or map
    Return plant-connection merged with connections info")
  (close-connection [plant-connection]
    "Connection cleanup")
  (get-exchange [plant-connection]
    "Return rmq exchange name associated with this connection")
  (get-channel [plant-connection]
    "Return channel associated with this connection")
  (get-connection [plant-connection]
    "Return rmq connection associated with this connection")
  (get-host [plant-connection]
    "Return host used for this connection")
  (get-port [plant-connection]
    "Return port used for this connection")
  (subscribe [plant-connection routing-key listener]
    "Subscribe to the routing key. Returns a map of subscription info")
  (cancel-subscription [plant-connection]
    "Cancels the first subscription that this object holds, if any")
  )

; Plant Interface is a collection of messages exchanged between planner components and plant implementation.
; Messages are sent over the wire in json format. Currently, we use rabbit mq for messaging between all components.
; All json keys and values are assumed to be required unless noted as optional.
; All timestamps are assumed to be unix time in millis since epoch (1 jan 1970)
; Examples of all messages are below and are in clojure format.

(defprotocol plantI
  ; Methods for various plant messages
  ; All the messages have an implicity field/slot :state.
  (start [plant-connection plant-id id function-name args argsmap plant-part timestamp]
    "Command to the plant to start the function identified by 'function-name' and args.
     Args are specified by both positional 'args' and named args via 'argsmap'
     plant-id is the unique identifier of each plant. The value comes from :id field as specified in pamela model.
     This message is published using plant-id as routing key or #plant if there is no :id in the pamela model.
     id uniquely identifies a invocation of a function. Multiple invocations of the same function will have different ids.
     :state :start")

  ; Messages intended for the plant are published using plant's routing key (value of :id field from the pamela language)
  ; Messages published by the plant are published with routing key :observations .
  (started [plant-connection plant-id id timestamp]
    "Reply to the start command
    :state started")

  (status-update [plant-connection plant-id id completion-time percent-complete timestamp]
    "Used by the plant to provide status updates.
    To provide completion estimate of the function when available.
    percent-complete field/slot is optional
    :state :status-update")

  (finished [plant-connection plant-id id reason timestamp]
    "Used by the plant to indicate that the function has completed.
    reason should a map as {:finish-state :success}. There could be additional fields as necessary.
    :state :finished")

  (failed [plant-connection plant-id id reason timestamp]
    "Used by the plant to indicate that the function has failed.
    reason should a map as {:finish-state :failed}. There could be additional fields as necessary.
    :state :finished")

  (cancel [plant-connection plant-id id timestamp]
    "Command to the plant to cancel a currently executing function.
    :state :cancel")

  (cancelled [plant-connection plant-id id reason timestamp]
    "Used by the plant to indicate that the function has been cancelled.
    reason should a map as {:finish-state :cancelled}. There could be additional fields as necessary.
    :state :finished")

  (observations [plant-connection plant-id id observations timestamp]
    "Used by the plant to provide observations about it's state
    id in this case is optional. i.e It can be nil and the message may not have this field/slot.
    observations should be a vector of maps. [TODO]
    :state :observations
    "))

; Plant Interface messages and routing keys

; 1. Name of the method being invoked and args
; Published using id of the plant as routing key or #plant if there is no id.
;{:plant-id :myplant :id 47 :state :start :function-name hardwork :args [10] :argsmap {:time 10} :timestamp 123}

; 2. Message from the plant that the method has started
; published by all plants with routing key #observations
;{:plant-id :myplant  :id 47 :state :started :timestamp 12345}

; 3. Message from the plant providing status update for the method
; :percent-complete is optional
; published by all plants with routing key #observations
;{:plant-id :myplant  :id 47 :state :status-update :timestamp 12345 :estimated-completion-time 12345 :percent-complete 12}

; 4. Message from the plant that the method has finished successfully
; :reason and :finish-state are required.
; Other key value pairs for :reason are optional
; published by all plants with routing key #observations
;{:plant-id :myplant  :id 47 :state :finished :timestamp 12345 :reason {:finish-state :success
;                                                   :str          "Finished Successfully"
;                                                   }}

; 5. Message from the plant that the method has finished with a failure
; :reason and :finish-state are required.
; Other key value pairs for :reason are optional
; published by all plants with routing key #observations
;{:plant-id :myplant  :id 47 :state :finished :timestamp 12345 :reason {:finish-state :failed
;                                                   :str          "file not found"}}

; 6. Message to the plant to cancel method execution
; Published using id of the plant as routing key or #plant if there is no id.
;{:plant-id :myplant  :id 47 :state :cancel}

; 7. Message from the plant that the method has been cancelled
; :reason and :finish-state are required.
; Other key value pairs for :reason are optional
; published by all plants with routing key #observations
;{:plant-id :myplant  :id 47 :state :finished :timestamp 12345 :reason {:finish-state :cancelled
;                                                   :str          "Cancelled per user request"}}

; 8. Plant observations
; An observation message must have atleast one observation
; published by all plants with routing key #observations
;{:plant-id :myplant :id 47 :state :observations [{:field :ball-in-motion :value true :timestamp 12345}
;                              {:field :ready :value false :timestamp 12345}
;                              ]}