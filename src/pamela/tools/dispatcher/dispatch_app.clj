;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.dispatcher.dispatch-app
  "Main TPN Dispatch app. "
  (:require [pamela.tools.utils.rabbitmq :as rmq]
            [pamela.tools.plant.connection :as plant]
            [pamela.tools.plant.interface :as plant_i]
            [pamela.tools.plant.util :as putil]

            [pamela.tools.dispatcher.dispatch :as dispatch]
            [pamela.tools.dispatcher.tpn-walk :as tpn_walk]
            [pamela.tools.dispatcher.tpn-import :as tpn_import]
            [pamela.tools.utils.tpn-json :as tpn-json]
            [pamela.tools.utils.util :as util]
            [pamela.tools.dispatcher.planviz :as planviz]
            [pamela.tools.utils.tpn-types :as tpn_type]
            [pamela.tools.utils.timer :as pt-timer]
    ;:reload-all ; Causes problems in repl with clojure multi methods.
            [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.pprint :refer :all]
            [clojure.walk])

  (:gen-class))

; forward declarations
(declare act-cancel-handler)
(declare act-failed-handler)
(declare handle-tpn-failed)
(declare publish-mission-updates-message)

(def debug false)
(def collect-bindings? true)
(def timeunit :secs)
(def cancel-tc-violations false)                            ;when true, activities that violate upper bounds will be cancelled.

(def mission-id nil)
(def force-plant-id false)
(def monitor-mode nil)
(def no-tpn-publish false)                                  ;we always publish tpn by default.
; To turn on a hack. Assumes arguments of type string are reference fields.
(def assume-string-as-field-reference false)

; To wait for tpn-dispatch command.
(def wait-for-tpn-dispatch false)

(def with-dispatcher-manager false)
(def dispatcher-manager-rkey "dispatcher-manager")
(def app-id "Dispatcher")

(defonce state (atom {}))
(defonce rt-exceptions (atom []))                           ; Using for testing tpn-dispatch
(defonce stop-when-rt-exceptions true)
(defonce activity-started-q (async/chan))
(defonce observations-q (async/chan))
(defonce tpn-failed-cb nil)                                 ; when tpn is failed, this callback will be called
(defonce tpn-finished-cb nil)                               ; when tpn is finishes, this callback will be called
(defonce bindings nil)

(def default-exchange "tpn-updates")
(def routing-key-finished-message "tpn.activity.finished")

(defn set-tpn-failed-handler [fn]
  (def tpn-failed-cb fn))

(defn set-tpn-finished-handler [fn]
  (def tpn-finished-cb fn))

(defn reset-rt-exception-state []
  (reset! rt-exceptions []))

(defn set-node-bindings [bndgs]
  (def bindings bndgs))

(defn exit []
  ;(println "repl state" (:repl @state))
  (when (and (contains? @state :repl)
             (false? (:repl @state))
             (do (println "Exit dispatcher\n")
                 (System/exit 0))))
  (println "In repl. Not Exiting\n"))

(defn get-tpn []
  (:tpn-map @state))

(defn get-network-id []
  (get-in @state [:tpn-map :network-id]))

(defn update-state! [m]
  (swap! state merge @state m))

(defn print-state []
  (println "state -")
  (pprint @state)
  (println "state --"))

(update-state! {:repl true})

(defn get-exchange-name []
  (:exchange @state))

(defn get-channel [exch-name]
  (get-in @state [exch-name :channel]))

(defn get-planviz []
  (:planviz @state))

(defn publish-message [data rkey]
  (rmq/publish-message data app-id rkey
                       (pt-timer/get-unix-time)
                       (get-channel (get-exchange-name))
                       (get-exchange-name)))

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default default-exchange]
                  ["-m" "--monitor-tpn" "Will monitor and update TPN state but not dispatch the begin tpn" :default false]
                  [nil "--no-tpn-publish" "When specified, we will not publish full tpn network" :default no-tpn-publish]
                  [nil "--assume-string-as-field-reference" "Assumes fields of type string are reference fields.
                        Will query BSM for values before dispatch" :default false]
                  ["-c" "--auto-cancel" "Will send cancel message to plant, when activities exceed their upper bounds" :default false]
                  ["-w" "--wait-for-dispatch" "Will wait for tpn dispatch command" :default false]
                  [nil "--force-plant-id" "Will override plant-id specified in TPN with plant" :default nil] ; For regression testing
                  [nil "--dispatch-all-choices" "Will dispatch all choices" :default false]
                  [nil "--simulate-clock" "Will listen for clock messages on rkey `clock` and use the clock for timeouts only!" :default false]
                  [nil "--with-dispatcher-manager" "Will publish TPN failed/finished state with reason" :default false]
                  [nil "--bindings-file file-name" "Will use the bindings specified in json file" :default nil]
                  [nil "--mission-id mission-id" "Optional mission-id when needed" :default nil]
                  ["-?" "--help"]])

#_(defn parse-summary [args]
    (clojure.string/join " " (map (fn [opt]
                                    (str (:long-opt opt) " " (if (:default opt)
                                                               (str "[" (:default opt) "]")
                                                               (:required opt)))) args)))

(defn reset-network-publish [netid & [ids]]
  (let [netid     (if-not netid
                    (get-network-id)
                    netid)

        ids       (if-not ids
                    (tpn_walk/collect-tpn-ids netid (:tpn-map @state))
                    ids)

        st        (apply merge (map (fn [id]
                                      {id {:uid              id
                                           :tpn-object-state :normal}})
                                    ids))
        exch-name (get-exchange-name)
        channel   (get-channel exch-name)]

    #_(pprint st)
    ; TODO refactor reset to a method
    (rmq/publish-object (merge st {:network-id netid}) "network.reset"
                        channel exch-name)))

(defn reset-network
  ([net-objs]
   (let [netid (:network-id net-objs)
         ids   (tpn_walk/collect-tpn-ids netid net-objs)]
     (reset-network ids netid)))
  ([ids netid]
   (println "Reset network (internal)")
   (dispatch/reset-state)
   ;; reset state in viewer
   (reset-network-publish netid ids)))

(defn toMsecs [time unit]
  (println "time and unit" time unit)
  (if (= unit :secs)
    (* time 1000)
    time))

(defn is-primitive [activity]
  (let [non-primitive (:non-primitive activity)]
    (if (nil? non-primitive)
      (do (println "activity :non-primitive is nil. Assuming primitive" activity)
          true)
      (not non-primitive))))

(defn get-plant-interface []
  (:plant-interface @state))

(defn get-plant-id [act-obj]
  (let [pl-id (or (:plantid act-obj) (:plant-id act-obj))
        pl-id (if pl-id
                pl-id
                "plant")]
    pl-id))

(defn publish-activity-to-plant [plant act-id]
  ;(println "publish-activity-to-plant" act-id)
  (let [tpn           (get-tpn)
        invocation-id (putil/make-method-id (str (name (get tpn :network-id)) "-" (name act-id) "-" putil/counter-prefix))
        act-obj       (get tpn act-id)
        plant-id      (if-not force-plant-id (get-plant-id act-obj)
                                             "plant")
        command       (:command act-obj)
        args          (or (:args act-obj) [])
        argsmap       (or (:argsmap act-obj) {})
        plant-part    (:plant-part act-obj)]
    (update-state! {invocation-id (:uid act-obj)})          ;When a plant sends observations about this invocation id, we find find corresponding act-id
    (dispatch/update-plant-dispatch-id act-id invocation-id)
    (plant_i/start plant (name plant-id) invocation-id command args argsmap plant-part nil)))

(defn arg-value-reference-type? [arg]
  ;(or (str/starts-with? arg ":") (str/includes? arg ".:"))
  (when (and (string? arg)
             (or (str/includes? arg ".:")
                 (str/starts-with? arg ":")))
    (println "Warning: Assuming argument is of reference type" arg)
    true))

(defn parse-reference-field
  "Assume field is of reference type. If it has .:, then it is of form
  plant-id.:field-name
  Otherwise it is of form :field-name
  retun [plant-id-or-nil field-name]" [arg]
  (if (str/includes? arg ".:")
    (str/split arg #".:")
    [nil (str/replace arg #":" "")]))

(defn group-by-value-or-reference
  "for each activity, if any of its args is of type java.lang.String and contains '.:' or begins_with ':', then assume actvity has reference field(s),
   otherwise all are values."
  [act-id]
  (let [act-obj (get (:tpn-map @state) act-id)
        args    (:args act-obj)]
    ;(println "Got act = " act-id args)
    (if (some arg-value-reference-type? args)
      :reference-type :value-type)))

(defn query-belief-state
  "Given activity has atleast one arg that is of reference type"
  [plant act-id]

  (let [act-obj (get (:tpn-map @state) act-id)
        args    (:args act-obj)]
    ;(println )
    ;(pprint act-obj)
    (doseq [arg args]
      (when (arg-value-reference-type? arg)

        (let [split-vals            (parse-reference-field arg)
              belief-state-plant-id "bsm1"
              for-plant-id          (or (first split-vals) (get-plant-id act-obj)) ; not forcing plant-id for belief state TBD
              field-name            (second split-vals)
              invocation-id         (putil/make-method-id "bs1-query-id")]
          ;(println "field references" split-vals args)
          (println "Query belief state: " act-id " arg:" arg "for-plant-id" for-plant-id)
          (update-state! {invocation-id [act-id arg]})      ; so that we can update bound values for reference fields.
          (plant_i/start plant (name belief-state-plant-id) invocation-id "get-field-value" [for-plant-id field-name]
                         {:plant-id   for-plant-id
                          :field-name field-name} nil nil))))))

(defn publish-to-plant [tpn-activities]
  ; when received, update the value in :tpn-map
  ; and call this function. use plant interface queue to ensure all incoming values are synchronized
  (let [plant (:plant-interface @state)]
    ;(pprint tpn-activities)
    (when plant
      (let [grouped     (if assume-string-as-field-reference
                          (group-by group-by-value-or-reference (keys tpn-activities))
                          {:value-type (keys tpn-activities)})

            value-types (:value-type grouped)
            ref-types   (:reference-type grouped)]
        ;(println "Grouped by value or reference")
        ;(pprint grouped)
        ;(println "value-types" value-types)
        ;(println "reference-types" ref-types)
        (doseq [act-id value-types]
          ;(println act-id)
          (publish-activity-to-plant plant act-id))
        (doseq [act-id ref-types]
          (query-belief-state plant act-id))))))

(defn get-reached-state [n]
  (nth (:reached-state @state) n))

(defn collect-bindings [before after]
  (when-not (contains? @state :bindings)
    (update-state! {:bindings []}))
  (update-state! {:bindings (conj (:bindings @state) {:before before :after after})}))

(defn cancel-plant-activities [act-ids time-millis]
  (let [plant (:plant-interface @state)]
    (when plant
      (doseq [act-id act-ids]
        (let [act     (get-in @state [:tpn-map act-id])
              disp-id (dispatch/get-plant-dispatch-id act-id)]
          (when disp-id
            (println "Sending cancel message" act-id disp-id)
            (plant_i/cancel plant (if-not force-plant-id (get-plant-id act)
                                                         "plant")
                            disp-id time-millis)))))))

(defn publish-metrics-observations [metrics]
  (let [metrics (clojure.walk/prewalk-replace {Double/POSITIVE_INFINITY "infinity"
                                               Double/NEGATIVE_INFINITY "-infinity"} metrics)
        exch    (get-exchange-name)
        chan    (get-channel exch)]
    ;(println "publish-metrics-observations\n" metrics)
    (rmq/publish-object metrics "planner.metrics" chan exch)))

(defn show-tpn-execution-time []
  (let [tpn-map    (:tpn-map @state)
        net-obj    ((:network-id tpn-map) tpn-map)
        begin-uid  (:begin-node net-obj)
        end-uid    (:end-node net-obj)
        node-times (dispatch/get-node-started-times tpn-map)
        time       (float (- (end-uid node-times) (begin-uid node-times)))]
    (println "TPN execution time:" time)
    time))

(defn show-activity-execution-times []
  (doseq [[uid obj] (:tpn-map @state)]
    (let [tpn-type   (:tpn-type obj)
          start-time (get-in @dispatch/state [uid :start-time])
          stop-time  (get-in @dispatch/state [uid :end-time])]

      (when (contains? tpn_type/edgetypes tpn-type)
        (if-not (and start-time stop-time)
          (println "activity:" uid "does not has start-time and stop-time")
          (let [exec-time (float (- stop-time start-time))]
            (if (> exec-time 0)
             (println "activity execution time:"
                      uid
                      (if (:display-name obj) (str "(" (:display-name obj) ")") "(null-activity)")
                      exec-time))))))))

(defn tpn-finished-execution? []
  (dispatch/network-finished? (:tpn-map @state)))

(defn stop-tpn-processing? []
  (let [rt-ex @rt-exceptions]
    (when (pos? (count rt-ex))
      (println "Error: Runtime exceptions" (count rt-ex))
      (doseq [ex rt-ex]
        (println "Exception: " (.getMessage ex)))
      (when stop-when-rt-exceptions
        (println "stop-when-rt-exceptions is" stop-when-rt-exceptions)
        (println "further activity dispatch should stop")
        true))))

(defn wait-until-tpn-finished
  "Blocking function to wait for tpn to finish"
  [& [until-abs-time-millis]]
  (println "Blocking function to wait until TPN is finished or timeout " until-abs-time-millis)

  (loop []
    (if until-abs-time-millis (println "Until timeout" (float (/
                                                                (- until-abs-time-millis
                                                                   (System/currentTimeMillis))
                                                                1000))))

    (let [timed-out?    (if until-abs-time-millis (>= (System/currentTimeMillis) until-abs-time-millis)
                                                  false)
          tpn-finished? (tpn-finished-execution?)
          more-wait?    (not (or tpn-finished? timed-out?))]
      ;(println "(stop-tpn-processing?)" (stop-tpn-processing?))
      ;(println "tpn-finished-execution?" (tpn-finished-execution?))
      ;(println "wait-until-tpn-finished timed-out?" timed-out?)
      ;(println "more wait" more-wait? "\n")
      (when-not more-wait?
        (println "wait-until-tpn-finished is DONE")
        {:stop-tpn-processing (stop-tpn-processing?)
         :tpn-finished        (tpn-finished-execution?)
         :timed-out           (and until-abs-time-millis (>= until-abs-time-millis (System/currentTimeMillis)))})
      (when more-wait?
        (Thread/sleep 1000)
        (recur)))))

(defn get-tpn-dispatch-state []
  (let [old-state (get-in @state [:tpn-dispatch])
        old-state (if-not old-state
                    [] old-state)]
    old-state))

(defn update-tpn-dispatch-state! [new-state]
  (update-state! {:tpn-dispatch new-state}))

(defn tpn-finished-helper
  "To be called when the tpn has finished or failed"
  [netid]
  (println "handle-tpn-finished helper" netid)
  (println "Network end-node reached. TPN Execution finished" netid)

  (publish-mission-updates-message)

  (show-activity-execution-times)
  (show-tpn-execution-time)

  (let [old-state (get-tpn-dispatch-state)
        old-info  (last old-state)
        new-info  (conj old-info {:end-time (pt-timer/getTimeInSeconds)})
        old-state (into [] (butlast old-state))
        new-state (conj old-state new-info)]
    (when (:dispatch-id old-info)
      (println "command finished" new-info)
      (plant_i/finished (:plant-interface @state) (name (:plant-id old-state)) (:dispatch-id old-state) nil nil))
    (update-tpn-dispatch-state! new-state)))

(defn handle-tpn-finished [netid]

  (tpn-finished-helper netid)

  (when with-dispatcher-manager
    (publish-message {:id     netid
                      :tpn    (get-tpn)
                      :state  :finished
                      :reason {:finish-state :success}}
                     dispatcher-manager-rkey))

  (when tpn-finished-cb
    (tpn-finished-cb (get-tpn)))

  (exit))

(defn handle-tpn-failed [tpn node-state fail-reasons]
  (println "TPN Failed" (:network-id tpn))
  ;(println "node state")
  ;(pprint node-state)
  (println "TPN Fail reason")
  (pprint fail-reasons)

  (tpn-finished-helper (:network-id tpn))

  (when with-dispatcher-manager
    (publish-message {:id     (:network-id tpn)
                      :tpn    tpn
                      :state  :finished
                      :reason {:finish-state :failed
                               :node-state   node-state
                               :fail-reasons fail-reasons}}
                     dispatcher-manager-rkey))

  (if tpn-failed-cb
    (tpn-failed-cb tpn node-state fail-reasons))

  (exit))

(defn get-temporal-bounds [act-id tpn]
  (let [x       (get-in tpn [act-id :constraints])
        cnst-id (if x (first x))]
    (if cnst-id (:value (cnst-id tpn)))))

(defn handle-activity-timeout
  "To be called when an activity has temporal bounds"
  [act-id]
  (act-failed-handler act-id :timeout))

(defn schedule-activity-timeouts [activities tpn]
  (println "schedule-activity-timeouts")
  ;(pprint activities)
  (doseq [act-id (keys activities)]
    (let [bounds (get-temporal-bounds act-id tpn)]
      (println "Activity bounds" act-id ":" bounds)
      (when bounds
        (let [before (pt-timer/get-unix-time)]
          (pt-timer/schedule-task (fn []
                                    #_(let [now (pt-timer/make-instant (pt-timer/get-unix-time))]
                                        (println act-id "timed out"
                                                 "\nstart:" (str (pt-timer/make-instant before))
                                                 "\nnow: " (str now)
                                                 "\nDuration" (str (java.time.Duration/between (pt-timer/make-instant before) now))))
                                    (handle-activity-timeout act-id))
                                  (* 1000 (second bounds))))))))

(defn make-current-next-other-activities []
  ; get all act ids. exclude null and delay activities
  ; remove finished activities
  ; then remove dispatched
  ; then remove next-actions
  ; left over is other actions

  (let [tpn                (get-tpn)
        next-acts          (:next-actions @state)
        acts               (util/filter-activities tpn)
        act-ids            (into #{} (map :uid acts))
        act-state          (dispatch/get-activities-state tpn)
        dispatched         (dispatch/get-dispatched-activities tpn)
        dispatched-ids     (into #{} (map :uid dispatched))
        next-activities    (reduce (fn [res act]
                                     (let [act-id (:uid act)]
                                       (into res (act-id next-acts))))
                                   [] dispatched)
        next-activities-id (into #{} (map :uid next-activities))
        finished-ids       (into #{} (keys (filter (fn [[_ state]]
                                                     (= :finished state))
                                                   act-state)))
        other-act-ids      (remove finished-ids act-ids)
        other-act-ids      (remove dispatched-ids other-act-ids)
        other-act-ids      (remove next-activities-id other-act-ids)
        current-actions    (select-keys tpn dispatched-ids)
        current-actions    (reduce (fn [res [aid act]]
                                     (conj res {aid (conj act {:plant-invocation-id (dispatch/get-plant-dispatch-id aid)})}))
                                   {} current-actions)]
    {
     ;:all-activities act-ids
     ;:finished-acts finished-ids
     ;:act-state act-state
     :mission-id          mission-id
     :tpn                 nil
     :current-actions     current-actions
     :next-actions        (select-keys tpn next-activities-id)
     :other-possibilities (select-keys tpn other-act-ids)}))

(defn publish-mission-updates-message []
  (publish-message (make-current-next-other-activities)
                   "dispatch-mission-updates"))

(defn publish-dispatched
  "Called whenever an change in activity state causes change in TPN state!"
  [dispatched tpn-net]
  (let [netid          (:network-id tpn-net)
        {tpn-activities true tpn-objects false} (group-by (fn [[_ v]]
                                                            (if (= :negotiation (:tpn-object-state v))
                                                              true
                                                              false))
                                                          dispatched)
        exch-name      (get-exchange-name)
        channel        (get-channel exch-name)
        ; group-by returns vectors. we need maps.
        tpn-activities (apply merge (map (fn [[id v]]
                                           {id v})
                                         tpn-activities))
        delays         (filter (fn [[id v]]
                                 (if (and (= :delay-activity (:tpn-type (id tpn-net)))
                                          (= :started (:tpn-object-state v)))
                                   true
                                   false))
                               dispatched)]

    ; TODO Refactor update, negotiation and finished event publish to methods.
    (when (pos? (count tpn-objects))
      (rmq/publish-object (merge (into {} tpn-objects) {:network-id netid}) "tpn.object.update" channel exch-name))

    (when (pos? (count tpn-activities))
      (rmq/publish-object (merge tpn-activities {:network-id netid}) "tpn.activity.negotiation" channel exch-name)
      (publish-to-plant tpn-activities))

    (schedule-activity-timeouts tpn-activities tpn-net)
    ; if dispatched has any delay activities, then create a timer to finish them.
    (doseq [a-vec delays]
      (let [id     (first a-vec)
            cnst   (get-temporal-bounds id tpn-net)
            lb     (first cnst)
            msec   (toMsecs lb timeunit)
            obj    {:network-id netid (first a-vec) {:uid (first a-vec) :tpn-object-state :finished}}
            before (pt-timer/get-unix-time)]
        (println "Starting delay activity" id "delay in millis:" msec)
        (pt-timer/schedule-task (fn []
                                  (println "delay finished" id "delayed by " (- (pt-timer/get-unix-time) before))
                                  (rmq/publish-object obj routing-key-finished-message channel exch-name))
                                msec)))
    (publish-mission-updates-message)
    (when (dispatch/network-finished? tpn-net)
      (handle-tpn-finished netid))))

(defn monitor-mode-publish-dispatched [dispatched tpn-net]
  (pprint "Monitor mode publish dispatched before")
  ;(pprint dispatched)
  ;(pprint "after")
  ;(pprint (into {} (remove (fn [[k v]]
  ;                              (= :activity (:tpn-type (get-object k tpn-net))))
  ;                            dispatched)))
  ; In Monitor mode, we don't dispatch activities but we need to publish node state
  (publish-dispatched (into {} (remove (fn [[k _]]
                                         (= :activity (:tpn-type (util/get-object k tpn-net))))
                                       dispatched)) tpn-net))

(defn act-finished-handler [act-id act-state tpn-map m]
  (let [before (pt-timer/getTimeInSeconds)]
    ;(println "begin -- act-finished-handler" act-id act-state (.getName (Thread/currentThread)))
    (cond (or (= :finished act-state) (= :success act-state) (= :cancelled act-state))
          (if monitor-mode
            (monitor-mode-publish-dispatched (dispatch/activity-finished (act-id tpn-map) tpn-map m) tpn-map)
            (publish-dispatched (dispatch/activity-finished (act-id tpn-map) tpn-map m) tpn-map))
          :else
          (util/to-std-err (println "act-finished-handler unknown state" act-state)))
    ;(println "act-finished-handler" act-id act-state "process time" (float (- (tutil/getTimeInSeconds) before)))
    (println "act-finished-handler end -- activity execution time" act-id (:display-name (util/get-object act-id tpn-map))
             (dispatch/get-activity-execution-time act-id))))

(defn act-do-not-wait-handler [act-id _ tpn-map m]
  ;dispatch/do-not-wait
  (let [x (dispatch/activity-do-not-wait (act-id tpn-map) tpn-map m)]
    ;(pprint x)
    (when (pos? (count x))
      (publish-dispatched x tpn-map))))

(defn act-cancel-handler [acts]
  (let [time (pt-timer/getTimeInSeconds)]
    (dispatch/cancel-activities acts time)
    (planviz/cancel (:planviz @state) acts (get-network-id))
    (cancel-plant-activities acts (* time 1000))))

(defn act-failed-handler [act-id reason]
  (let [tpn      (get-tpn)
        act-obj  (util/get-object act-id tpn)
        ; only a started activity can fail.
        ; activity in any other state such as cancel, cancelled, or finished cannot fail.
        started? (dispatch/activity-started? act-obj)]

    (when started?
      (println "act failed:" act-id reason)
      (let [fail-time  (pt-timer/getTimeInSeconds)
            ; Calling get-node-started-time before activity-failed because activity-failed
            ; updates node time for all the nodes that could possibly fail along the path
            node-state (dispatch/get-node-started-times tpn)
            ; the received node-state does not has node-state for act-objs
            node-state (merge node-state {(:end-node act-obj) fail-time})
            ;_ (do (println "node-state")
            ;      (pprint node-state))
            failed-ids (dispatch/activity-failed act-id tpn fail-time reason)
            act-obj (util/get-object act-id (:tpn-map @state))
            act-label  (:display-name act-obj)
            act-args (:args act-obj)]

        (when failed-ids
          (println "Not dispatching rest of activities as activity failed" act-id ":" act-label act-args)
          ;(println "failed ids" (count failed-ids) failed-ids)
          (planviz/failed (get-planviz) failed-ids (get-network-id))
          #_(dispatch/failed-objects failed-ids (util/getTimeInSeconds))
          (when (dispatch/network-finished? (get-tpn))
            (println "Network failed")
            (let [fail-reasons (dispatch/get-fail-reason (get-tpn))]
              ;(dispatch/print-state-internal (get-tpn))
              #_(doseq [[nid time-val] node-state]
                  (println "act-fail-handler" nid ":" (str (pt-timer/make-instant time-val))))
              #_(pprint node-state)
              (handle-tpn-failed (get-tpn) node-state fail-reasons))))))))





(defn get-non-plant-msg-type
  "Non plant message is:
  {:network-id :net-sequence.feasible,
   :act-3 {:uid :act-3, :tpn-object-state :finished}}"
  [msg]
  (->> msg
       (map (fn [[_ v]]
              (if (and (map? v)
                       (contains? v :tpn-object-state))
                (:tpn-object-state v))))
       (remove nil?)
       (first)))

(defmulti handle-activity-message
          "Dispatch function for various activity messages"
          (fn [msg]
            (get-non-plant-msg-type msg)))

(defmethod handle-activity-message :default [msg]
  (util/to-std-err (println "handle-activity-message")
                   (pprint msg)))

;no-op because we publish this in response to started message from plant so that
; planviz can update it's state.
(defmethod handle-activity-message :started [_])

(defmethod handle-activity-message :finished [msg]
  ;(pprint msg)
  (doseq [[_ v] msg]
    (if (:tpn-object-state v)
      (act-finished-handler (:uid v)
                            (:tpn-object-state v)
                            (:tpn-map @state)
                            (select-keys @state [:choice-fn])))))

(defmethod handle-activity-message :failed [msg]
  ;(pprint msg)
  (doseq [[_ v] msg]
    (when (:tpn-object-state v)
      #_(println "handle-activity-message :failed" (:uid v))
      (act-failed-handler (:uid v) :other))))

(defmethod handle-activity-message :cancelled [msg]
  ;(pprint msg)
  (doseq [[_ v] msg]
    (if (:tpn-object-state v)
      (act-finished-handler (:uid v)
                            (:tpn-object-state v)
                            (:tpn-map @state)
                            (select-keys @state [:choice-fn])))))

(defmethod handle-activity-message :do-not-wait [msg]
  "This message implies that tpn can continue forward.
  Pretend as if the activity has finished"
  (doseq [[_ v] (filter #(map? (second %)) msg)]
    (if (and (:tpn-object-state v) (tpn_type/edgetypes (:tpn-type (util/get-object (:uid v) (:tpn-map @state)))))
      (act-do-not-wait-handler (:uid v)
                               (:tpn-object-state v)
                               (:tpn-map @state)
                               (select-keys @state [:choice-fn]))
      (println "handle-activity-message :do-not-wait does not apply for:" (:uid v) (tpn_type/edgetypes (:tpn-type (util/get-object (:uid v) (:tpn-map @state))))))))


(defmethod handle-activity-message :cancel-activity [msg]
  "Published from planviz when the user wishes to cancel the activity.
  "
  (doseq [[_ v] msg]
    (if (:tpn-object-state v)
      (act-cancel-handler [(:uid v)]))))

(defn process-activity-msg
  "To process the message received from RMQ"
  [msg]
  (let [last-msg (:last-rmq-msg @state)]
    (when last-msg
      (binding [*out* *err*]
        (println "last rmq message. Sync incoming rmq messages")
        (pprint (:last-rmq-msg @state)))))

  (update-state! {:last-rmq-msg msg})
  ;(println " process-activity-msg Got message")
  ;(pprint msg)
  (handle-activity-message msg)
  (update-state! {:last-rmq-msg nil}))


(defn process-activity-started
  "Called when in monitor mode"
  [act-msg]
  (pprint act-msg)
  (let [network-id  (get-in @state [:tpn-map :network-id])
        act-network (:network-id act-msg)
        m           (get @state :tpn-map)]

    (when (= act-network network-id)
      (println "Found network " network-id)
      (let [network-obj  (util/get-object network-id m)
            begin-node   (util/get-object (:begin-node network-obj) m)
            acts-started (disj (into #{} (keys act-msg)) :network-id)]

        (if (dispatch/network-finished? m)
          (reset-network m))

        (if (dispatch/node-dispatched? begin-node m)
          (println "Begin node is dispatched")
          (do (println "Begin node is not dispatched")
              ; We are not passing choice fn as last parameter because we are in monitor mode and not making any decisions.
              (let [dispatched (dispatch/dispatch-object network-obj m {})]
                (println "Updating state of rest")
                (pprint (apply dissoc dispatched acts-started))

                (monitor-mode-publish-dispatched (apply dissoc dispatched acts-started) m))))))))

(defn activity-started-handler [m]
  (when monitor-mode
    (async/>!! activity-started-q m)))


(defn setup-activity-started-listener []
  "Assume the network is stored in app atom (@state)"
  (async/go-loop [msg (async/<! activity-started-q)]
    (if-not msg
      (println "Blocking Queue is closed for receiving messages 'RMQ activity started'")
      (do
        (process-activity-started msg)
        (recur (async/<! activity-started-q)))))

  (update-state! {:activity-started-listener (rmq/make-subscription "tpn.activity.active"
                                                                    (fn [_ _ ^bytes payload]
                                                                      (let [data (String. payload "UTF-8")
                                                                            m    (tpn-json/map-from-json-str data)]
                                                                        (println "Got activity started message")
                                                                        (activity-started-handler m)))

                                                                    (get-channel (get-exchange-name))
                                                                    (:exchange @state))}))

; Callback To receive messages from RMQ
(defn act-finished-handler-broker-cb [payload]
  ;(println "act-finished-handler-broker-cb recvd from rmq: " payload)
  (process-activity-msg (rmq/payload-to-clj payload)))

(defn setup-broker-cb []
  (rmq/make-subscription routing-key-finished-message
                         (fn [_ _ ^bytes payload]
                           (act-finished-handler-broker-cb payload))
                         (get-channel (get-exchange-name))
                         (:exchange @state)))

(defn dispatch-tpn [tpn-net]
  ;(reset-network tpn-net)
  ;(Thread/sleep 200)
  (let [netid      (:network-id tpn-net)
        network    (netid tpn-net)
        dispatched (dispatch/dispatch-object network tpn-net {})]
    (update-state! {:next-actions (util/make-next-actions tpn-net)})
    (println "Dispatching netid" netid (util/getCurrentThreadName))
    (publish-dispatched dispatched tpn-net)))

(defn dispatcher-plant-command-handler [payload]
  (let [cmd (rmq/payload-to-clj payload)]
    (println "Got dispatcher command" (util/getCurrentThreadName))
    (pprint cmd)
    (when (and (= :start (:state cmd))
               (= "dispatch-tpn" (:function-name cmd)))
      (let [time      (pt-timer/getTimeInSeconds)
            old-state (get-tpn-dispatch-state)
            old-info  (last old-state)
            new-state (conj old-state {:start-time  time
                                       :dispatch-id (:id cmd)})]
        (when (and old-info (not (contains? old-info :end-time)))
          (util/to-std-err (println "Warn: Dispatching tpn but previous dispatch not finished." old-info)))

        (update-tpn-dispatch-state! new-state)
        (dispatch-tpn (:tpn-map @state))
        (plant_i/started (:plant-interface @state) (name (:plant-id cmd)) (:id cmd) nil)))))

(defn setup-dispatcher-command-listener []
  (rmq/make-subscription "dispatcher"
                         (fn [_ _ ^bytes payload]
                           (dispatcher-plant-command-handler payload))
                         (get-channel (get-exchange-name))
                         (:exchange @state)))

(defn init-new-tpn [tpn]
  (println "init-new-tpn")
  (update-state! {:tpn-map tpn})

  (when-not no-tpn-publish
    (println "Publishing network")
    (rmq/publish-object tpn "network.new" (get-channel (get-exchange-name)) (get-exchange-name)))

  (reset-network tpn))

(defn setup-and-dispatch-tpn-with-bindings [tpn bindings abs-dispatch-time]
  ;(println "setup-and-dispatch-tpn-with-bindings")
  ;(println "bindings")
  ;(pprint bindings)
  (init-new-tpn tpn)
  (update-state! {:tpn-bindings bindings :tpn-dispatch-time abs-dispatch-time})
  (dispatch/set-node-bindings bindings)
  (dispatch-tpn tpn))

(defn setup-and-dispatch-tpn [tpn-net]
  #_(println "Dispatching TPN from file" file)
  ; Sequence of steps when dispatching TPN from file
  (println "Use Ctrl-C to exit")
  (init-new-tpn tpn-net)

  (cond (true? monitor-mode)
        (println "In Monitor mode. Not dispatching")

        (true? wait-for-tpn-dispatch)
        (println "Waiting for tpn dispatch command. Not dispatching")

        :else
        (if bindings
          (do (setup-and-dispatch-tpn-with-bindings tpn-net bindings nil))
          (dispatch-tpn tpn-net))))

(defn usage [options-summary]
  (->> ["TPN Dispatch Application"
        ""
        "Usage: java -jar tpn-dispatch-XXX-standalone.jar [options] tpn-File.json"
        ""
        "Options:"
        options-summary
        ""]

       (string/join \newline)))

(defn get-tpn-file [args]                                   ;Note args are (:arguments parsed)

  (if (< (count args) 1)
    [nil "Need tpn-File.json"]
    (do
      (if (and (> (count args) 0) (.exists (io/as-file (first args))))
        [(first args) nil]
        [nil (str "File not found:" " " (first args))]))))


(defn publish-activity-state [id state net-id routing-key]
  "To use when plant observations come through"
  (let [exch (get-exchange-name)
        ch   (get-channel exch)]
    (rmq/publish-object {:network-id net-id
                         id          {:uid              id
                                      :tpn-object-state state}}
                        routing-key ch exch)))

(defn is-bsm-reply-msg [msg]
  (and (= :bsm1 (:plant-id msg))
       (not (keyword? ((:id msg) @state)))))

(defn get-plant-finished-state [msg]
  (keyword (str (name (:state msg)) "-" (name (get-in msg [:reason :finish-state])))))

(defmulti handle-plant-message
          "Dispatch fn for various incoming plant messages"
          (fn [msg]
            (let [state (:state msg)]
              (cond (= :started state)
                    :started
                    (= :status-update state)
                    :status-update
                    (= :finished state)
                    (get-plant-finished-state msg)))))

(defmethod handle-plant-message :default [msg]
  (util/to-std-err
    (println "handle-plant-message " (:state msg))
    (pprint msg)))

(defmethod handle-plant-message :started [msg]
  (println "plant activity started invocation-id act-id" (:id msg) ((:id msg) @state))
  (publish-activity-state ((:id msg) @state) :started (get-network-id) routing-key-finished-message))

(defmethod handle-plant-message :finished-success [msg]
  (println "plant activity finished" (:id msg) ((:id msg) @state))
  (publish-activity-state ((:id msg) @state) :finished (get-network-id) routing-key-finished-message))

(defmethod handle-plant-message :finished-cancelled [msg]
  (println "plant activity cancelled" (:id msg) ((:id msg) @state))
  (publish-activity-state ((:id msg) @state) :cancelled (get-network-id) routing-key-finished-message))

(defmethod handle-plant-message :finished-canceled [msg]
  (println "plant activity canceled" (:id msg) ((:id msg) @state) "This is temporary handler. Will be removed soon!")
  (publish-activity-state ((:id msg) @state) :cancelled (get-network-id) routing-key-finished-message))

(defmethod handle-plant-message :finished-failed [msg]
  (println "activity finished with fail state" ((:id msg) @state))
  (publish-activity-state ((:id msg) @state) :failed (get-network-id) routing-key-finished-message))

(defmethod handle-plant-message :status-update [_])         ;NOOP

(defn handle-plant-reply-msg [msg]
  ;(println "handle-plant-reply-msg")
  ;(pprint msg)
  (when (and (:id msg) ((:id msg) @state))
    (handle-plant-message msg))
  (when (and (not (:id msg))
             (pos? (count (:observations msg))))
    (doseq [obs (:observations msg)]
      (when (= "tpn-object-state" (:field obs))
        (handle-activity-message (:value obs))))))

(defn handle-bsm-reply-msg [msg]
  (when (= :finished (:state msg))
    (let [invocation-id     (:id msg)
          invocation-detail (invocation-id @state)
          act-id            (first invocation-detail)
          replacement-arg   (second invocation-detail)
          new-field-value   (get-in msg [:reason :value])
          old-args          (get-in @state [:tpn-map act-id :args])
          new-args          (replace {replacement-arg new-field-value} old-args)]
      ; Note. Not updating argsmap as it is deprecated

      ;(println "Replacing old with new args" old-args new-args)
      ;(println "before activity" )
      ;(pprint (get-in @state [:tpn-map act-id]))
      (swap! state assoc-in [:tpn-map act-id :args] new-args)
      ;(swap! state assoc-in [:tpn-map act-id :argsmap] new-args)
      ;(println "after activity" )
      ;(pprint (get-in @state [:tpn-map act-id]))

      (if (nil? new-field-value)
        (println "Error: BSM returned nil value " invocation-detail))

      (when (and new-field-value (not-any? arg-value-reference-type? new-args))
        (when (:plant-interface @state)
          (println "Bound all reference fields from bsm:" act-id old-args "->" new-args)
          (publish-activity-to-plant (:plant-interface @state) act-id))))))


(defn handle-observation-message [msg]
  ;(println "Observation message")
  ;(pprint msg)
  ;(Thread/sleep 250)                                         ;only for help with legible printing
  (if (is-bsm-reply-msg msg)
    (handle-bsm-reply-msg msg)
    (handle-plant-reply-msg msg)))

(defn setup-recv-observations-from-q []
  (do (async/go-loop [msg (async/<! observations-q)]
        (if-not msg
          (println "Blocking Queue is closed for receiving #observation messages")
          (do
            (handle-observation-message msg)
            (recur (async/<! observations-q)))))
      (update-state! {:observations-q-setup true})))

(defn put-rmq-message [data q]
  (async/>!! q (tpn-json/map-from-json-str (String. data "UTF-8"))))

(defn setup-plant-interface [exch-name host port]
  ;(pprint @state)
  (if-not (:observations-q-setup @state)
    (do
      (println "Setting up observations q")
      (update-state! {:plant-interface (plant/make-plant-connection exch-name {:host host :port port})})
      (setup-recv-observations-from-q)
      (rmq/make-subscription "observations" (fn [_ _ data]
                                              (put-rmq-message data observations-q))
                             (get-in @state [:plant-interface :channel]) (:exchange @state)))
    (println "Observations q is already setup. ")))

(defn setup-planviz-interface [channel exchange]
  (when (contains? @state :planviz)
    (println "Warn: state has planviz object set. " (:planviz @state)))
  (swap! state assoc :planviz (planviz/make-rmq channel exchange)))

(defn reset-state []
  (println "reset-state")
  ;(pprint (keys @state))
  (let [repl (:repl @state)]
    ;(reset! state {}); FIXME causes RMQ connections and call back to be persistent in background and asynchrony issues in repl!
    (update-state! {:repl repl})))

(defn init []
  (reset-state)
  ;(reset-agent-state)
  (reset-rt-exception-state))

(defn setup-rmq [exch-name host port]
  (let [m (rmq/make-channel exch-name {:host host :port port})]
    (if (:channel m)
      (do (update-state! {(:exchange m) m})
          (setup-plant-interface exch-name host port)
          (setup-planviz-interface (:channel m) exch-name)

          ;; Guard so that we do not subscribe everytime we run from repl.
          ;; We expect to run only once from main.
          (when-not (:broker-subscription @state)
            (println "Setting subscription")
            ;(msg-serial-process)
            (setup-broker-cb)

            (if monitor-mode
              (setup-activity-started-listener))
            (update-state! {:broker-subscription true}))

          (when-not (:dispatcher-plant @state)
            (println "Setting dispatcher command listener")
            (setup-dispatcher-command-listener)
            (update-state! {:dispatcher-plant true}))
          #_(print-state))
      (do
        (println "Error creating rmq channel")
        (exit)))))

(defn update-clock [_ _ data]
  (let [msg (util/map-from-json-str (String. data "UTF-8"))
        ts  (:timestamp msg)]
    (if ts (pt-timer/update-clock ts))))

(defn go [& [args]]
  (let [parsed                         (cli/parse-opts args cli-options)
        help                           (get-in parsed [:options :help])
        errors                         (:errors parsed)
        [tpn-file _] (get-tpn-file (:arguments parsed))
        tpn-network                    (when tpn-file (tpn_import/from-file tpn-file))
        exch-name                      (get-in parsed [:options :exchange])
        host                           (get-in parsed [:options :host])
        port                           (get-in parsed [:options :port])
        monitor                        (get-in parsed [:options :monitor-tpn])
        no-tpn-pub                     (get-in parsed [:options :no-tpn-publish])
        auto-cancel                    (get-in parsed [:options :auto-cancel])
        wait-for-dispatch              (get-in parsed [:options :wait-for-dispatch])
        force-plant-id-local           (get-in parsed [:options :force-plant-id])
        dispatch-all-choices           (get-in parsed [:options :dispatch-all-choices])
        sim-clock                      (get-in parsed [:options :simulate-clock])
        mission-id-local               (get-in parsed [:options :mission-id])
        with-dispatcher-manager-option (get-in parsed [:options :with-dispatcher-manager])
        bindings-file-option           (get-in parsed [:options :bindings-file])]


    ;(pprint parsed)
    (when help
      (println (usage (:summary parsed)))
      (exit))

    (when errors
      (println (usage (:summary parsed)))
      (println (string/join \newline errors))
      (exit))

    (println "dispatch-all-choices" dispatch-all-choices)
    (dispatch/set-dispatch-all-choices dispatch-all-choices)

    (println "mission-id" mission-id-local)
    (def mission-id mission-id-local)
    (init)

    (if-not monitor-mode
      (def monitor-mode monitor))

    (if (true? force-plant-id-local)
      (def force-plant-id true)
      (def force-plant-id false))
    (println "Will force plant id to be plant" force-plant-id)

    (def no-tpn-publish no-tpn-pub)
    (println "Will publish? tpn" (not no-tpn-publish))

    (def cancel-tc-violations auto-cancel)
    (def wait-for-tpn-dispatch wait-for-dispatch)

    (def assume-string-as-field-reference (get-in parsed [:options :assume-string-as-field-reference]))
    (println "Applying hack? assume-string-as-field-reference" assume-string-as-field-reference)

    (def with-dispatcher-manager with-dispatcher-manager-option)
    (println "With Dispatcher manager" with-dispatcher-manager)
    (when bindings-file-option
      (def bindings (:bindings (tpn-json/read-bindings-from-json bindings-file-option)))
      (println "Using bindings file: " bindings-file-option)
      (pprint bindings))
    #_(clojure.pprint/pprint parsed)
    #_(clojure.pprint/pprint tpn-network)
    (update-state! (:options parsed))
    #_(print-state)

    (setup-rmq exch-name host port)
    (pt-timer/set-use-sim-time sim-clock)
    (when sim-clock
      (rmq/make-subscription "clock" update-clock (get-channel exch-name) exch-name)
      ; to let published clock sync
      (Thread/sleep 2000))

    (if tpn-file
      (do
        (update-state! {:tpn-file tpn-file})
        (setup-and-dispatch-tpn tpn-network))
      (do
        (println "No TPN File given! Will wait for external TPN.")))))

; Dispatch TPN, Wait for TPN to finish and Exit.
(defn -main
  "Dispatches TPN via RMQ"
  [& args]
  ;(println "TPN Dispatch args" args)
  (update-state! {:repl false})
  (go args))

; 4/11/19
;  - Dispatcher should exit when all activities are finished or failed.
;  - Before exiting it should present accurate view of TPN dispatch state to planviz
;  -

; Command line parsing.
; https://github.com/clojure/tools.cli
; TODO Controllable activities should be cancelled when it's upper bound is expired.

; TODO / FIXME "Network end-node reached. TPN Execution finished :net-3"
; This should happen only once. See dance/demo-july-2017.tpn.json