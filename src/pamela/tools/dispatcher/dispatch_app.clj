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
            [pamela.tools.mct-planner.solver :as solver]
            [pamela.tools.mct-planner.expr :as expr]
    ;:reload-all ; Causes problems in repl with clojure multi methods.
            [clojure.string :as string]
            [ruiyun.tools.timer :as timer]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.pprint :refer :all]
            [clojure.set :as set]
            [clojure.walk])
  (:gen-class))

; forward declarations
(declare act-cancel-handler)

(def debug false)
(def collect-bindings? true)
(def timeunit :secs)
(def cancel-tc-violations false)                            ;when true, activities that violate upper bounds will be cancelled.

(def monitor-mode nil)
(def no-tpn-publish false)                                  ;we always publish tpn by default.
; To turn on a hack. Assumes arguments of type string are reference fields.
(def assume-string-as-field-reference false)

; To wait for tpn-dispatch command.
(def wait-for-tpn-dispatch true)

(defonce state (atom {}))
(defonce rt-exceptions (atom []))                           ; Using for testing tpn-dispatch
(defonce stop-when-rt-exceptions true)
(defonce activity-started-q (async/chan))
(defonce observations-q (async/chan))
; Event handler agent. To simply serialize all events
(defonce event-handler (agent {}))

(def default-exchange "tpn-updates")
(def routing-key-finished-message "tpn.activity.finished")

(defn reset-rt-exception-state []
  (reset! rt-exceptions []))

(defn show-agent-error []
  (println (agent-error event-handler)))

(defn reset-agent-state
  "To be used when running from repl or unit tests to reset agent-state"
  []
  ;(println @event-handler)
  (when (agent-error event-handler)
    (show-agent-error)
    (restart-agent event-handler {} :clear-actions true)))

;;; Solver timer helpers
(defn make-solver-timer []
  (timer/deamon-timer "Solver timer"))

(defonce solver-timer (make-solver-timer))
(def solver-period 2000)
(def throttle-time 1000)

(defn reset-solver-timer! []
  (def solver-timer (make-solver-timer)))

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
; Solver makes choice decisions now. Leaving as reference. 9/22/2017
#_(update-state! {:repl      true
                  :choice-fn tpn.dispatch/first-choice
                  })
;(print-state)

(defn get-exchange-name []
  (:exchange @state))

(defn get-channel [exch-name]
  (get-in @state [exch-name :channel]))

(defn get-planviz []
  (:planviz @state))

(def cli-options [["-h" "--host rmqhost" "RMQ Host" :default "localhost"]
                  ["-p" "--port rmqport" "RMQ Port" :default 5672 :parse-fn #(Integer/parseInt %)]
                  ["-e" "--exchange name" "RMQ Exchange Name" :default default-exchange]
                  ["-m" "--monitor-tpn" "Will monitor and update TPN state but not dispatch the begin tpn" :default false]
                  [nil "--no-tpn-publish" "When specified, we will not publish full tpn network" :default no-tpn-publish]
                  [nil "--assume-string-as-field-reference" "Assumes fields of type string are reference fields.
                        Will query BSM for values before dispatch" :default false]
                  ["-c" "--auto-cancel" "Will send cancel message to plant, when activities exceed their upper bounds" :default false]
                  ["-w" "--wait-for-dispatch" "Will wait for tpn dispatch command" :default false]
                  [nil "--disable-periodic-solver" "Will not run solver periodically" :default false]
                  ["-?" "--help"]])

#_(defn parse-summary [args]
    (clojure.string/join " " (map (fn [opt]
                                    (str (:long-opt opt) " " (if (:default opt)
                                                               (str "[" (:default opt) "]")
                                                               (:required opt)))
                                    ) args)))
(defn reset-network-publish [netid & [ids]]
  (let [netid (if-not netid
                (get-network-id)
                netid)

        ids (if-not ids
              (tpn_walk/collect-tpn-ids netid (:tpn-map @state))
              ids)

        st (apply merge (map (fn [id]
                               {id {:uid              id
                                    :tpn-object-state :normal}})
                             ids))
        exch-name (get-exchange-name)
        channel (get-channel exch-name)
        ]
    #_(pprint st)
    ; TODO refactor reset to a method
    (rmq/publish-object (merge st {:network-id netid}) "network.reset"
                        channel exch-name)))

(defn reset-network
  ([net-objs]
   (let [netid (:network-id net-objs)
         ids (tpn_walk/collect-tpn-ids netid net-objs)]
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
  (let [tpn (get-tpn)
        invocation-id (putil/make-method-id (str (name (get tpn :network-id))  "-" (name act-id) "-" putil/counter-prefix) )
        act-obj (get tpn act-id)
        plant-id (get-plant-id act-obj)
        command (:command act-obj)
        args (or (:args act-obj) [])
        argsmap (or (:argsmap act-obj) {})
        plant-part (:plant-part act-obj)]
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
        args (:args act-obj)]
    ;(println "Got act = " act-id args)
    (if (some arg-value-reference-type? args)
      :reference-type :value-type)))

(defn query-belief-state
  "Given activity has atleast one arg that is of reference type"
  [plant act-id]

  (let [act-obj (get (:tpn-map @state) act-id)
        args (:args act-obj)]
    ;(println )
    ;(pprint act-obj)
    (doseq [arg args]
      (when (arg-value-reference-type? arg)

        (let [split-vals (parse-reference-field arg)
              belief-state-plant-id "bsm1"
              for-plant-id (or (first split-vals) (get-plant-id act-obj))
              field-name (second split-vals)
              invocation-id (putil/make-method-id "bs1-query-id")]
          ;(println "field references" split-vals args)
          (println "Query belief state: " act-id " arg:" arg "for-plant-id" for-plant-id)
          (update-state! {invocation-id [act-id arg]})      ; so that we can update bound values for reference fields.
          (plant_i/start plant (name belief-state-plant-id) invocation-id "get-field-value" [for-plant-id field-name]
                                 {:plant-id   for-plant-id
                                  :field-name field-name} nil nil))))))

(defn publish-to-plant [tpn-activities]
  ; when received, update the value in :tpn-map
  ; and call this function. use plant interface queue to ensure all incoming values are synchronized
  ;
  (let [plant (:plant-interface @state)]
    ;(pprint tpn-activities)
    (when plant
      (let [grouped (if assume-string-as-field-reference
                      (group-by group-by-value-or-reference (keys tpn-activities))
                      {:value-type (keys tpn-activities)})

            value-types (:value-type grouped)
            ref-types (:reference-type grouped)]
        ;(println "Grouped by value or reference")
        ;(pprint grouped)
        ;(println "value-types" value-types)
        ;(println "reference-types" ref-types)
        (doseq [act-id value-types]
          ;(println act-id)
          (publish-activity-to-plant plant act-id))
        (doseq [act-id ref-types]
          (query-belief-state plant act-id)))
      )))

(defn get-exprs []
  (get-in @state [:expr-details :all-exprs]))

(defn get-nid-2-var []
  (get-in @state [:expr-details :nid-2-var]))

(defn get-tc-uid-for-expr
  "Return temporal-constraint uid or nil"
  [expr]
  {:pre [(util/check-type pamela.tools.mct_planner.expr.expr expr)]}           ;When this condition fails, code stops and no meaningful information is given.
  ;(pprint expr)
  ;(println (type expr) "\n" (instance? repr.expr.expr expr))
  ;(println expr)
  (get-in expr [:m :temporal-constraint]))

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
        (let [act (get-in @state [:tpn-map act-id])
              disp-id (dispatch/get-plant-dispatch-id act-id)]
          (when disp-id
            (println "Sending cancel message" act-id disp-id)
            (plant_i/cancel plant (get-plant-id act) disp-id time-millis)))))))

(defn throttle-feasibility-helper []
  (let [last-time (:last-solver-time @state)
        elapsed (if last-time
                  (- (System/currentTimeMillis) last-time)
                  throttle-time)
        throttling (and last-time (> throttle-time elapsed))]
    ;(println "Elapsed " elapsed throttling)
    #_(when throttling
        (println "Throttling feasibility helper because:" elapsed))
    throttling))

(defn publish-metrics-observations [metrics]
  (let [metrics (clojure.walk/prewalk-replace {java.lang.Double/POSITIVE_INFINITY "infinity"
                                               java.lang.Double/NEGATIVE_INFINITY "-infinity"
                                               } metrics)
        plnt (get-plant-interface)
        exch (get-exchange-name)
        chan (get-channel exch)]
    ;(println "publish-metrics-observations\n" metrics)
    ;(println "exch" exch)
    ;(println "chan" chan)
    (rmq/publish-object metrics "planner.metrics" chan exch)
    #_(plant_i/observations plnt nil "planner-metrics" [(plant/make-observation :planner-metrics metrics)] nil)))

; Running constraint solver
; Create initial bindings when TPN is dispatched.
; Update bindings before dispatching next activity?
(defn check-and-report-infeasibility-helper [tpn-map exprs reached-state]
  (try (let [start (System/currentTimeMillis)]
         (println "start -- check-and-report-infeasibility-helper" (util/getCurrentThreadName))
         (if-not (throttle-feasibility-helper)
           (let [nid2-var (get-in @state [:expr-details :nid-2-var])
                 nid-2-var-range-x (dispatch/nid-2-var-range nid2-var)
                 initial-bindings (dispatch/temporal-bindings-from-tpn-state nid-2-var-range-x reached-state)
                 #_ (do (println "initial bindings")
                        (pprint initial-bindings))
                 sample (if (pos? (count initial-bindings))
                          (solver/solve exprs (get-nid-2-var) 1 initial-bindings)
                          (solver/solve exprs (get-nid-2-var) 1))

                 metrics (solver/collect-metrics sample)

                 sample (first sample)
                 new-bindings (:bindings sample)

                 expr-state (:satisfies sample)
                 ; Group expressions into normal and failed where failed is any activity that has started but violated its constraint.
                 norm-failed (group-by (fn [[expr value]]
                                         (cond (and (false? value) (dispatch/get-start-time (expr/get-tpn-uid expr)))
                                               :failed
                                               ; Note that a expr that has been cancelled before it has started,
                                               ; will have 0 execution time units and hence will be marked as failed if lower-bound is > 0.
                                               ; Even though expr has failed mathematically, it is not failed as it never started.
                                               :else
                                               :normal)) expr-state)
                 failed-exprs (:failed norm-failed)
                 norm-exprs (:normal norm-failed)

                 failed-objs (reduce (fn [res [expr _]]
                                       (conj res (get-in expr [:m :object])))
                                     #{} failed-exprs)

                 failed-tc-ids (reduce (fn [res [expr _]]
                                         (let [uid (get-tc-uid-for-expr expr)]
                                           (if uid
                                             (conj res uid)
                                             res)))
                                       #{}  failed-exprs)

                 normal-tc-ids (reduce (fn [res [expr _]]
                                         (let [uid (get-tc-uid-for-expr expr)]
                                           (if uid
                                             (conj res uid)
                                             res)))
                                       #{}  norm-exprs)
                 planviz (:planviz @state)]

             ;(println "check-and-report-infeasibility")
             ;(println "Failed exprs")
             ;(pprint failed-exprs)
             ;(println "Failed objects" failed-objs)

             ; Helpful for debugging
             ;(println "expr-state")
             ;(pprint expr-state)
             ;(println "Bindings for vars")
             ;(pprint new-bindings)
             ;(println "nid-2-var-range-x")
             ;(pprint nid-2-var-range-x)
             ;(println "Bindings for objs")
             ;(pprint (set/rename-keys new-bindings (get-in @state [:expr-details :var-2-nid])))

             (let [prev-reached-state (:reached-state @state)
                   updated (if prev-reached-state
                             (conj prev-reached-state reached-state)
                             [reached-state])]
               (update-state! {:reached-state updated}))
             (when collect-bindings?
               (collect-bindings initial-bindings new-bindings))

             (when debug
               (println "expr initial bindings")
               (pprint initial-bindings)
               (println "expr new bindings")
               (pprint new-bindings)

               (println "normal uids:" normal-tc-ids)
               (println "Failed expression ids:" failed-tc-ids)
               (println "Bound expr count: " (count (:expr-values sample)))
               (doseq [exp (:expr-values sample)]
                 (pprint (first exp))
                 (println (if (ratio? (second exp))
                            (float (second exp))
                            (second exp)))))

             ; i.e Send state info about all exprs to planviz, not just failed exprs.
             (if planviz

               (do
                 (planviz/normal planviz normal-tc-ids (:network-id tpn-map))
                 (planviz/failed planviz failed-tc-ids (:network-id tpn-map)))
               (println "Planviz reference is not valid"))

             (when cancel-tc-violations
               ;; Find not-finished activities
               (let [dispatched (dispatch/get-dispatched-activities tpn-map)
                     ids (reduce (fn [res act]
                                   (conj res (:uid act))) #{} dispatched)
                     cancellable (set/intersection ids failed-objs)]
                 ;(println "Dispatched" ids)
                 ;(println "Cancellable" cancellable)
                 (act-cancel-handler cancellable)))

             ; publish metrics of the sample
             ;(publish-metrics-observations metrics); TODO FIXME Exception in thread "Solver timer" java.lang.Exception: Don't know how to write JSON of class clojure.lang.Atom

             (update-state! {:last-solver-time (System/currentTimeMillis)})))
         (println "end -- check-and-report-infeasibility-helper"
                  (float (/ (- (System/currentTimeMillis) start) 1000)) (util/getCurrentThreadName)))
       (catch Exception e
         (swap! rt-exceptions conj e)
         (util/to-std-err (println "Exception: ")
                                           (.getMessage e)
                                           (.printStackTrace e)
                                           ))))

(defn activity-ready-for-solver?
  "activity must have dispatched. i.e dispatch has :start-time"
  [act now tpn-map]
  (let [bounds (:value (first (util/get-constraints (:uid act) tpn-map)))
        lb (first bounds)
        start-time (dispatch/get-start-time (:uid act))]
    ;(println "activity-ready-for-solver? lb" (:uid act) lb)
    #_(println "now - start-time" (float (- now (dispatch/get-start-time (:uid act)))))
    (cond (not start-time)
          false

          (nil? lb)
          true

          (and lb
               start-time
               (>= (- now start-time) lb))                  ;activity has started and time elapsed is >= lower-bound
          true

          :else                                             ; time elapsed < lower-bound
          false)))

;;; Find dispatched activities.
;;; Assume current time as end-time of the activities
;;; Ensure now - act-starttime > lower-bound
;;; Run solver with appropriate bindings
;;;
(defn periodic-solver []
  (let [now (util/getTimeInSeconds)
        _ (println "Running periodic solver" now)
        tpn-map (:tpn-map @state)
        exprs (get-exprs)
        dispatched (dispatch/get-unfinished-activities tpn-map)
        dispatched (filter (fn [act]
                             (activity-ready-for-solver? act now tpn-map)) dispatched)
        end-nodes (map (fn [act]
                         (:end-node act)) dispatched)
        reached-state (dispatch/get-node-started-times tpn-map)

        reached-state (reduce (fn [result nid]
                                (conj result {nid now})) reached-state end-nodes)]
    ;(println "Dispatched")
    ;(pprint dispatched)
    (check-and-report-infeasibility-helper tpn-map exprs reached-state)))

(defn schedule-solver
  "Setup the timer to call solver periodically"
  []
  (timer/run-task! periodic-solver :by solver-timer :period 2000 :delay 1000))

(defn stop-solver
  "Cancels the timer that calls solver periodically"
  []
  (timer/cancel! solver-timer)
  (reset-solver-timer!)
  (println "solver stopped"))

(defn check-and-report-infeasibility [& [event-handler]]
  (let [tpn-map (get-in @state [:tpn-map])
        exprs (get-exprs)
        reached-state (dispatch/get-node-started-times tpn-map)]
    (check-and-report-infeasibility-helper tpn-map exprs reached-state)))

(defn show-tpn-execution-time []
  (let [tpn-map (:tpn-map @state)
        net-obj ((:network-id tpn-map) tpn-map)
        begin-uid (:begin-node net-obj)
        end-uid (:end-node net-obj)
        node-times (dispatch/get-node-started-times tpn-map)
        time (float (- (end-uid node-times) (begin-uid node-times)))]
    (println "TPN execution time:" time)
    time))

(defn show-activity-execution-times []
  (doseq [[uid obj] (:tpn-map @state)]
    (let [tpn-type (:tpn-type obj)
          start-time (get-in @dispatch/state [uid :start-time])
          stop-time (get-in @dispatch/state [uid :end-time])]

      (when (contains? tpn_type/edgetypes tpn-type)
        (if-not (and start-time stop-time)
          (println "activity state has nil time(s)" start-time stop-time)
          (println "activity execution time:" uid (:display-name obj) (float (- stop-time start-time))))))))

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
        ;(stop-solver)
        true
        ))))

(defn wait-until-tpn-finished
  "Blocking function to wait for tpn to finish"
  [& [until-abs-time-millis]]
  (println "Blocking function to wait until TPN is finished or timeout " until-abs-time-millis)

  (loop []
    (if until-abs-time-millis (println "Until timeout" (float (/
                                                                (- until-abs-time-millis
                                                                   (System/currentTimeMillis))
                                                                1000))))

    (let [timed-out? (if until-abs-time-millis (>= (System/currentTimeMillis) until-abs-time-millis)
                                               false)
          tpn-finished? (tpn-finished-execution?)
          more-wait? (not (or tpn-finished? timed-out?))]
      ;(println "(stop-tpn-processing?)" (stop-tpn-processing?))
      ;(println "tpn-finished-execution?" (tpn-finished-execution?))
      ;(println "wait-until-tpn-finished timed-out?" timed-out?)
      ;(println "more wait" more-wait? "\n")
      (when-not more-wait?
        (println "wait-until-tpn-finished Stopping solver")
        (stop-solver)
        (println "wait-until-tpn-finished is DONE")
        {:stop-tpn-processing (stop-tpn-processing?)
         :tpn-finished (tpn-finished-execution?)
         :timed-out (and until-abs-time-millis (>= until-abs-time-millis (System/currentTimeMillis)))})
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

(defn handle-tpn-finished [netid]
  (println "handle-tpn-finished" netid)
  (println "Network end-node reached. TPN Execution finished" netid)
  (show-activity-execution-times)
  (show-tpn-execution-time)
  (stop-solver)
  (let [old-state (get-tpn-dispatch-state)
        old-info (last old-state)
        new-info (conj old-info {:end-time (util/getTimeInSeconds)})
        old-state (into [] (butlast old-state))
        new-state (conj old-state new-info)]
    (when (:dispatch-id old-info)
      (println "command finished" new-info)
      ;TODO Finish state should match that of constraint solver.
      (plant_i/finished (:plant-interface @state) (name (:plant-id old-state)) (:dispatch-id old-state) nil nil))
    (update-tpn-dispatch-state! new-state))
  ;(println "Sleep before exit")
  ;(Thread/sleep 1000)
  (exit))

(defn publish-dispatched [dispatched tpn-net]
  ;(println "publish-dispatched dispatched" (:network-id tpn-net) (get-in @state [:tpn-map :network-id]) (.getName (Thread/currentThread)))
  ;(pprint dispatched)
  (let [netid (:network-id tpn-net)
        {tpn-activities true tpn-objects false} (group-by (fn [[_ v]]
                                                            ;(println "k=" k "v=" v)
                                                            (if (= :negotiation (:tpn-object-state v))
                                                              true
                                                              false
                                                              )) dispatched)
        exch-name (get-exchange-name)
        channel (get-channel exch-name)
        ; group-by returns vectors. we need maps.
        tpn-activities (apply merge (map (fn [[id v]]
                                           {id v}
                                           ) tpn-activities))
        delays (filter (fn [[id v]]
                         (if (and (= :delay-activity (:tpn-type (id tpn-net)))
                                  (= :started (:tpn-object-state v)))
                           true
                           false))
                       dispatched)]
    #_(println (into {} tpn-objects))
    #_(println (into {} tpn-activities))
    ; TODO Refactor update, negotiation and finished event publish to methods.
    ; FIXME make routing from TPN activity plant info
    (when (pos? (count tpn-objects))
      (rmq/publish-object (merge (into {} tpn-objects) {:network-id netid}) "tpn.object.update" channel exch-name))

    (when (pos? (count tpn-activities))
      (rmq/publish-object (merge tpn-activities {:network-id netid}) "tpn.activity.negotiation" channel exch-name)
      (publish-to-plant tpn-activities))

    ; if dispatched has any delay activities, then create a timer to finish them.
    (doseq [a-vec delays]
      (let [id (first a-vec)
            cnst-id (first (get-in tpn-net [id :constraints]))
            cnst (cnst-id tpn-net)
            lb (first (:value cnst))
            msec (toMsecs lb timeunit)
            obj {:network-id netid (first a-vec) {:uid (first a-vec) :tpn-object-state :finished}}
            before (System/currentTimeMillis)]
        (println "Starting delay activity" id "delay in millis:" msec)
        (timer/run-task! (fn []
                           (println "delay finished" id "delayed by " (- (System/currentTimeMillis) before))
                           (rmq/publish-object obj routing-key-finished-message channel exch-name))
                         :delay msec)))

    ; By now  dispatch/state should be uptodate. Check with constraint solver and update state constraint violations
    ;(println "Checking feasibility")
    (send event-handler check-and-report-infeasibility)
    ;(println "Checking feasibility -- done")
    ; See if end-node is reached
    ;(println "\nChecking network finished?")
    (send event-handler (fn [old-state]
                          (when (dispatch/network-finished? tpn-net)
                            (handle-tpn-finished netid)))))
  #_(if (stop-tpn-processing?)
    ; stop when there are exceptions and flag is set
    (do (stop-solver)
        nil)
    ; otherwise process
    ))

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
  (let [before (util/getTimeInSeconds)]
    (println "begin -- act-finished-handler" act-id act-state (.getName (Thread/currentThread)))
    ;(println "tpn-map")
    ;(pprint tpn-map)
    ;(println "m")
    ;(pprint m)
    (cond (or (= :finished act-state) (= :success act-state) (= :cancelled act-state))
          (if monitor-mode
            (monitor-mode-publish-dispatched (dispatch/activity-finished (act-id tpn-map) tpn-map m) tpn-map)
            (publish-dispatched (dispatch/activity-finished (act-id tpn-map) tpn-map m) tpn-map))
          :else
          (util/to-std-err (println "act-finished-handler unknown state" act-state)))
    ;(println "act-finished-handler" act-id act-state "process time" (float (- (tutil/getTimeInSeconds) before)))
    (println "end -- activity execution time" act-id (:display-name (util/get-object act-id tpn-map)) (dispatch/get-activity-execution-time act-id))
    ))

(defn act-do-not-wait-handler [act-id act-state tpn-map m]
  ;dispatch/do-not-wait
  (let [x (dispatch/activity-do-not-wait (act-id tpn-map) tpn-map m)]
    ;(pprint x)
    (when (pos? (count x))
      (publish-dispatched x tpn-map))))

(defn act-cancel-handler [acts]
  (let [time (util/getTimeInSeconds)]
    (dispatch/cancel-activities acts time)
    (planviz/cancel (:planviz @state) acts (get-network-id))
    (cancel-plant-activities acts (* time 1000))))

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

(defmethod handle-activity-message :started [msg]
  ;no-op because we publish this in response to started message from plant so that
  ; planviz can update it's state.
  )

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
      #_(println (select-keys @state [:choice-fn]))
      (println "Not dispatching rest of activities as activity failed" (:uid v) (:display-name (util/get-object (:uid v) (:tpn-map @state))))
      (let [failed-ids (dispatch/derive-failed-objects (get-tpn) (:uid v))]
        #_(println "failed ids" (count failed-ids) failed-ids)
        (planviz/failed (get-planviz) failed-ids (get-network-id))
        (dispatch/failed-objects failed-ids  (util/getTimeInSeconds))))))

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
      (println "handle-activity-message :do-not-wait does not apply for:" (:uid v) (tpn_type/edgetypes (:tpn-type (util/get-object (:uid v) (:tpn-map @state)))))
      )))

(defmethod handle-activity-message :cancel-activity [msg]
  "Published from planviz when the user wishes to cancel the activity.
  "
  (doseq [[_ v] msg]
    (if (:tpn-object-state v)
      (act-cancel-handler [(:uid v)]))))

(defn process-activity-msg
  "To process the message received from RMQ"
  ([msg]
   (let [last-msg (:last-rmq-msg @state)]
     (when last-msg
       (binding [*out* *err*]
         (println "last rmq message. Sync incoming rmq messages")
         (pprint (:last-rmq-msg @state))
         )))
   (update-state! {:last-rmq-msg msg})
    ;(println " process-activity-msg Got message")
    ;(pprint msg)
   (handle-activity-message msg)
   (update-state! {:last-rmq-msg nil}))

  ([old-state msg]                                          ;"When working on event-handler thread"
    ;(println "event-handler process-activity-msg")
   (process-activity-msg msg)))

(defn process-activity-started
  "Called when in monitor mode"
  [act-msg]
  (pprint act-msg)
  (let [network-id (get-in @state [:tpn-map :network-id])
        act-network (:network-id act-msg)
        m (get @state :tpn-map)]

    (when (= act-network network-id)
      (println "Found network " network-id)
      (let [network-obj (util/get-object network-id m)
            begin-node (util/get-object (:begin-node network-obj) m)
            acts-started (disj (into #{} (keys act-msg)) :network-id)
            ]

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
    (async/>!! activity-started-q m)
    ))

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
                                                                                         m (tpn-json/map-from-json-str data)]
                                                                                     (println "Got activity started message")
                                                                                     (activity-started-handler m)
                                                                                     ))
                                                                    (get-channel (get-exchange-name))
                                                                    (:exchange @state)
                                                                    )}))
; Callback To receive messages from RMQ
(defn act-finished-handler-broker-cb [payload]
  ;(println "act-finished-handler-broker-cb recvd from rmq: " payload)
  (send event-handler process-activity-msg (rmq/payload-to-clj payload)))

(defn setup-broker-cb []
  (rmq/make-subscription routing-key-finished-message
                         (fn [_ _ ^bytes payload]
                                        (act-finished-handler-broker-cb payload))
                         (get-channel (get-exchange-name))
                         (:exchange @state)))

(defn dispatch-tpn [tpn-net periodic-solver?]
  (reset-network tpn-net)
  (Thread/sleep 200)
  (let [netid (:network-id tpn-net)
        network (netid tpn-net)
        dispatched (dispatch/dispatch-object network tpn-net {})]
    (println "Dispatching netid" netid (util/getCurrentThreadName))
    (publish-dispatched dispatched tpn-net)
    (if (true? periodic-solver?)
      (schedule-solver))))

(defn dispatcher-plant-command-handler [payload]
  (let [cmd (rmq/payload-to-clj payload)]
    (println "Got dispatcher command" (util/getCurrentThreadName))
    (pprint cmd)
    (when (and (= :start (:state cmd))
               (= "dispatch-tpn" (:function-name cmd)))
      (let [time (util/getTimeInSeconds)
            old-state (get-tpn-dispatch-state)
            old-info (last old-state)
            new-state (conj old-state {:start-time  time
                                       :dispatch-id (:id cmd)})]
        (when (and old-info (not (contains? old-info :end-time)))
          (util/to-std-err (println "Warn: Dispatching tpn but previous dispatch not finished." old-info)))

        (update-tpn-dispatch-state! new-state)
        (send event-handler
              (fn [old-state]
                (dispatch-tpn (:tpn-map @state) false)      ;Do not run solver when bsm is dispatching tpn
                (plant_i/started (:plant-interface @state) (name (:plant-id cmd)) (:id cmd) nil)))))))

(defn setup-dispatcher-command-listener []
  (rmq/make-subscription "dispatcher"
                         (fn [_ _ ^bytes payload]
                                        (dispatcher-plant-command-handler payload))
                         (get-channel (get-exchange-name))
                         (:exchange @state)))

(defn setup-and-dispatch-tpn [tpn-net periodic-solver?]
  #_(println "Dispatching TPN from file" file)

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

  (update-state! {:tpn-map tpn-net})
  (update-state! {:expr-details (expr/make-expressions-from-map tpn-net)})
  (dispatch/set-tpn-info tpn-net (:expr-details @state))

  (let [exch-name (:exchange @state)
        channel (get-in @state [exch-name :channel])]
    (println "Use Ctrl-C to exit")

    (when-not no-tpn-publish
      (println "Publishing network")
      (rmq/publish-object tpn-net "network.new" channel exch-name))

    (reset-network tpn-net)

    ; Run constraint solver to render infeasibility
    (send event-handler check-and-report-infeasibility)

    (cond (true? monitor-mode)
          (println "In Monitor mode. Not dispatching")

          (true? wait-for-tpn-dispatch)
          (println "Waiting for tpn dispatch command. Not dispatching")

          :else
          (dispatch-tpn tpn-net periodic-solver?)
          )))

(defn usage [options-summary]
  (->> ["TPN Dispatch Application"
        ""
        "Usage: java -jar tpn-dispatch-XXX-standalone.jar [options] tpn-File.json"
        ""
        "Options:"
        options-summary
        ""
        ]
       (string/join \newline)))

(defn get-tpn-file [args]                                   ;Note args are (:arguments parsed)

  (if (< (count args) 1)
    [nil "Need tpn-File.json"]
    (do
      (if (and (> (count args) 0) (.exists (io/as-file (first args))))
        [(first args) nil]
        [nil (str "File not found:" " " (first args))]
        ))))

(defn publish-activity-state [id state net-id routing-key]
  "To use when plant observations come through"
  (let [exch (get-exchange-name)
        ch (get-channel exch)]
    (rmq/publish-object {:network-id net-id
                        id           {:uid              id
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

(defmethod handle-plant-message :status-update [msg]);NOOP

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
    (let [invocation-id (:id msg)
          invocation-detail (invocation-id @state)
          act-id (first invocation-detail)
          replacement-arg (second invocation-detail)
          new-field-value (get-in msg [:reason :value])
          old-args (get-in @state [:tpn-map act-id :args])
          new-args (replace {replacement-arg new-field-value} old-args)
          ; Note. Not updating argsmap as it is deprecated
          ]
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
  (update-state! {:plant-interface (plant/make-plant-connection exch-name {:host host :port port})})
  (if-not (:observations-q-setup @state)
    (do
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
  (let [repl (:repl @state)]
    (reset! state {})
    (update-state! {:repl repl})))

(defn go [& [args]]
  (reset-state)
  (reset-agent-state)
  (reset-rt-exception-state)
  (let [parsed (cli/parse-opts args cli-options)
        help (get-in parsed [:options :help])
        errors (:errors parsed)
        [tpn-file message] (get-tpn-file (:arguments parsed)) ;(get-in parsed [:options :tpn])
        tpn-network (when tpn-file (tpn_import/from-file tpn-file))
        exch-name (get-in parsed [:options :exchange])
        host (get-in parsed [:options :host])
        port (get-in parsed [:options :port])
        monitor (get-in parsed [:options :monitor-tpn])
        no-tpn-pub (get-in parsed [:options :no-tpn-publish])
        auto-cancel (get-in parsed [:options :auto-cancel])
        wait-for-dispatch (get-in parsed [:options :wait-for-dispatch])
        periodic-solver-disabled? (get-in parsed [:options :disable-periodic-solver])]

    (if-not monitor-mode
      (def monitor-mode monitor))

    (def no-tpn-publish no-tpn-pub)
    (println "Will publish? tpn" (not no-tpn-publish))

    (def cancel-tc-violations auto-cancel)
    (def wait-for-tpn-dispatch wait-for-dispatch)

    (def assume-string-as-field-reference (get-in parsed [:options :assume-string-as-field-reference]))
    (println "Applying hack? assume-string-as-field-reference" assume-string-as-field-reference)

    (println "periodic-solver-disabled?" periodic-solver-disabled?)
    (when errors
      (println (usage (:summary parsed)))
      (println (string/join \newline errors))
      (exit))

    (when help
      (println (usage (:summary parsed)))
      (exit))

    #_(clojure.pprint/pprint parsed)
    #_(clojure.pprint/pprint tpn-network)
    (update-state! (:options parsed))
    #_(print-state)

    (if tpn-file
      (do
        ; solver exprs.
        (update-state! {:tpn-file tpn-file})
        (println "Connecting to RMQ" host ":" port "topic" exch-name)
        (let [m (rmq/make-channel exch-name {:host host :port port})]
          (if (:channel m)
            (do (update-state! {(:exchange m) m})
                (setup-plant-interface exch-name host port)
                (setup-planviz-interface (:channel m) exch-name)
                #_(print-state)
                (setup-and-dispatch-tpn tpn-network (not periodic-solver-disabled?)))
            (do
              (println "Error creating rmq channel")
              (exit)))))
      (do
        (println (usage (:summary parsed)))
        (println message)
        (exit)
        ))))

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