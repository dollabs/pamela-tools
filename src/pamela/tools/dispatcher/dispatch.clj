;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.dispatcher.dispatch
  "Keeps track of dispatched state of TPN"
  (:require
    [pamela.tools.utils.timer :as pt-timer]
    [pamela.tools.utils.tpn-types :as tpn_types]
    [pamela.tools.utils.util :as util]

    [clojure.pprint :refer :all]
    [clojure.set :as set]))

; Forward declarations.
(declare activity-finished?)
(declare dispatch-object)

(defonce state (atom {}))
;(defonce tpn-info {})
(defonce dispatch-all-choices false)

(defn set-dispatch-all-choices [val]
  (def dispatch-all-choices val))

(defn make-printable-time-seconds [secs]
  (str (pt-timer/make-instant-seconds secs)))

(defn- update-state
  "uses update-in to update state atom"
  [keys value]
  (if-not value
    (util/to-std-err (println "Warn: update-state value is nil for keys:" keys)))
  ;(println "update-state" keys value (if (number? value) (make-printable-time-seconds value)))
  (let [pre-cond (every? #(not (nil? %)) keys)]
    (if-not pre-cond
      (util/to-std-err (println "Error: update-state at least one of the keys is nil: Won't update, Given keys" keys))
      (swap! state update-in keys (fn [_] value)))))

(defprotocol dispatchI
  "fns that work with dispatched state of TPN"
  (start [this uid time])
  (started [this uid time])
  (cancel [this uid time])
  (cancelled [this uid time])
  (failed [this uid time])
  (update-plant-dispatch-id-internal [this act-id disp-id])
  (get-plant-dispatch-id-internal [this act-id]))

(defrecord dispatchR []
  ;"For now attached to state atom"
  dispatchI
  (cancel [this act-id time]
    (update-state [act-id :cancel-time] time))
  (cancelled [this act-id time]
    (update-state [act-id :cancelled-time] time))
  (failed [this obj-id time]
    (if (nil? (get-in @state [obj-id :start-time]))
      ; we should not over write start-time for activities that have started
      ; nodes and activities that have not started will not have :start-time so
      ; we use given value for start-time, end-time and fail-time
      (update-state [obj-id :start-time] time))
    (update-state [obj-id :end-time] time)
    (update-state [obj-id :fail-time] time))
  (update-plant-dispatch-id-internal [this act-id disp-id]
    (update-state [act-id :plant-dispatched-id] disp-id))
  (get-plant-dispatch-id-internal [this act-id]
    (get-in @state [act-id :plant-dispatched-id])))

; Just an object to work with state atom
(def dispatch-state (->dispatchR))

(defn cancel-activities [uids time]
  (let [t (pt-timer/getTimeInSeconds {:time time})]
    (doseq [uid uids]
      (cancel dispatch-state uid t))))

(defn- failed-objects [uids time]
  (let [t (pt-timer/getTimeInSeconds {:time time})]
    (doseq [uid uids]
      (failed dispatch-state uid t))))

(defn update-plant-dispatch-id [act-id disp-id]
  (update-plant-dispatch-id-internal dispatch-state act-id disp-id))

(defn get-plant-dispatch-id [act-id]
  (get-plant-dispatch-id-internal dispatch-state act-id))

(defn reset-state []
  (println "dispatch/reset-state")
  (reset! state {}))

(defn print-node-bindings [bindings]
  (doseq [[nid {time-val :temporal-value}] bindings]
    ;(println nid time-val)
    (cond (number? time-val)
          (println nid ": temporal value: " (str (pt-timer/make-instant-seconds (long time-val))))
          (vector? time-val)
          (println nid ": temporal value: " (str "["
                                                 (pt-timer/make-instant-seconds (long (first time-val)))
                                                 " "
                                                 (if (= time-val java.lang.Double/POSITIVE_INFINITY)
                                                   (str time-val)
                                                   #_(pt-timer/make-instant (long (second time-val))))
                                                 "]")))))

(defn set-node-bindings
  "Provided by planner. Used to determine which choice to take"
  [bindings]
  ;(println "dispatch state")
  ;(pprint @state)
  ;(println "Node bindings")
  ;(print-node-bindings bindings)
  (update-state [:tpn-bindings] bindings))

#_(defn set-tpn-info [tpn-map exprs-details]
    (def tpn-info {:tpn-map      tpn-map
                   :expr-details exprs-details}))           ;

#_(defn- get-tpn []
    (:tpn-map tpn-info))

#_(defn- get-exprs []
    (get-in tpn-info [:expr-details :all-exprs]))

#_(defn- get-nid-2-var []
    (get-in tpn-info [:expr-details :nid-2-var]))

;; Solver stuff
(defn- node-object-started? [uid tpn-map]
  (let [obj           (uid tpn-map)
        known-object? (and (contains? @state uid)
                           (contains? tpn_types/nodetypes (:tpn-type obj)))]
    (cond known-object?
          (let [value (get-in @state [uid :start-time])]
            [(not (nil? value)) uid value])
          :else
          (do
            ;(println "object-reached?: unknown object" uid)
            [false uid]))))

(defn get-node-started-times [tpn-map]
  (let [reached (reduce (fn [result uid]
                          (conj result (node-object-started? uid tpn-map)))
                        [] (keys @state))]
    (reduce (fn [result triple]
              ;(println triple)
              (if (true? (first triple))
                (merge result {(second triple) (nth triple 2)})
                result
                )) {} reached)))

#_(defn- get-node-id-2-var []
    (get-in tpn-info [:expr-details :nid-2-var]))

#_(defn- get-choice-var [uid node-vars]
    (first (filter rutil/is-select-var? node-vars)))

(defn- find-activity [src-uid target-uid tpn-map]
  (let [acts     (:activities (util/get-object src-uid tpn-map))
        act-objs (map (fn [act-id]
                        (util/get-object act-id tpn-map)) acts)
        found    (filter (fn [act]
                           (= target-uid (:uid (util/get-end-node-activity act tpn-map))))
                         act-objs)]
    (first found)))

(defn get-choice-binding
  "Using bindings provided by external planner
  Return act-id"
  [src-nid target-nid tpn-map]
  ;(println "get-choice-binding" src-nid target-nid)
  (let [src-acts (:activities (util/get-object src-nid tpn-map))
        act-ids  (filter (fn [act-id]
                           (= target-nid (:end-node (util/get-object act-id tpn-map)))
                           ) src-acts)
        ]
    ;(println "outgoing acts" act-ids)
    (when (not= 1 (count act-ids))
      (println "get-choice-bindings for " src-nid target-nid) "there should be only 1 activity
      Got " act-ids)
    (first act-ids)))

(defn- reset-state-network [ids]
  "Walk the TPN for the given network and remove all objects from state"
  (util/remove-keys ids state))                             ;(walk/collect-tpn-ids netid objects)

(defn- update-dispatch-state! [uid key value]
  "Updates the runtime state of the object identified by uid"
  #_(println "update-state" uid key value)
  ;(println "update-state" [uid key] value (if (number? value) (str (pt-timer/make-instant-seconds value))))
  (if-not uid
    (util/debug-object (str "Cannot update run time state for nil uid " key " " value) nil update-dispatch-state!)
    (do (when (get-in @state [uid key])
          (println "Warning tpn.dispatch/state has [uid key] new-val" uid (get-in @state [uid key]) value))
        (swap! state assoc-in [uid key] value))))

(defn- simple-activity-dispatcher [act _ _]                 ;objs and m
  (println "simple-activity-dispatcher" (:uid act) "type" (:tpn-type act) (:name act)))

(defn- first-choice [activities _]
  "Return the first activity"
  (if (empty? activities)
    (util/debug-object "Activity list is empty" activities first-choice)
    (first activities)))

(defn- node-reached-helper [node state objs]
  "A node is reached if all the activities in the incidence set are finished."
  (let [pending-or-finished (group-by (fn [act-id]
                                        (if (activity-finished? (act-id objs) state)
                                          :finished :pending)) (:incidence-set node))
        pending-completion  (into #{} (:pending pending-or-finished))
        finished            (into #{} (:finished pending-or-finished))]

    (cond (and (= :c-end (:tpn-type node))
               (= false dispatch-all-choices))
          (do (if (> (count finished) 1)
                (println "For choice node" (:uid node) "finished count is greater than 1." finished))
              (println "Choice end node. pending and finished" pending-completion finished)
              [(>= (count finished) 1) pending-completion finished])
          :else
          [(empty? pending-completion) pending-completion finished]))
  ; TODO Impl block below in clojurish way and fixed a bug. Delete post 2021
  #_(with-local-vars [pending-completion #{}
                      finished           #{}]
      ; Split incidence set of activities into pending-completion and finished.
      (every? (fn [id]
                (let [finished? (activity-finished? (id objs) state)]
                  (if-not finished?
                    (var-set pending-completion (conj @pending-completion id))
                    (var-set finished (conj @finished id)))))
              (:incidence-set node))
      ;(println "Choice end node? " (= :c-end (:tpn-type node)))
      (if (= :c-end (:tpn-type node))
        (do
          (if (> (count @finished) 1)
            (println "For choice node" (:uid node) "finished count is greater than 1." @finished))
          (println "Choice end node.pending and finished" @pending-completion @finished)
          [(>= (count @finished) 1) @pending-completion @finished]
          )
        [(empty? @pending-completion) @pending-completion @finished])
      ))

; TODO Add to some protocol
(defn get-start-time [uid & [provided-state]]               ;TODO remove provided-state if not needed.
  (let [state (or provided-state @state)]
    (get-in state [uid :start-time])))

(defn- check-node-state [node state objs]
  {:pre [(map? node) (map? state) (map? objs)]}
  "Returns a [reached? #{activities pending completion} start-time (as-long or nil)"
  (let [nid        (:uid node)
        start-time (get-in state [nid :start-time])
        [reached pending-completion finished] (node-reached-helper node state objs)]
    #_(println "node-reached-helper returned reached? pending-completion? finished?" reached pending-completion finished)
    [reached pending-completion start-time finished]))

(defn- check-activity-state [act state]
  {:pre [(map? act) (map? state)]}
  "Returns one of :not-dispatched, :finished, :cancelled, :cancel, :dispatched, or :error.
  :not-dispatched if not found in state
  Relies on corresponding time to be in state to determine runtime state of the activity.
  "
  (let [id             (:uid act)
        start-time     (get-in state [id :start-time])
        end-time       (get-in state [id :end-time])
        cancel-time    (get-in state [id :cancel-time])
        cancelled-time (get-in state [id :cancelled-time])]
    #_(println "act start end " id start-time end-time)
    (cond (= nil (id state))
          (do
            ;(util/to-std-err (println "unknown activity" id))
            :not-dispatched)

          (not (nil? end-time))
          :finished

          (not (nil? cancelled-time))
          :cancelled

          (not (nil? cancel-time))
          :cancel

          (not (nil? start-time))
          :dispatched

          :otherwise
          (do (util/debug-object "Error in Activity state" act check-activity-state)
              :error))))

(defn get-activity-state [act]
  (check-activity-state act @state))

; TODO Add to some protocol
(defn node-dispatched?
  "A node is dispatched if it has start-time"
  ([node objs]
   (node-dispatched? node @state objs))
  ([node state objs] #_(println "node dispatched?" (:uid node))
   (nth (check-node-state node state objs) 2)))

(defn- node-reached? [node state objs]
  "A node is reached if all the activities in the incidence set are reached."
  #_(println "node reached?" (:uid node))
  (when node
    (first (check-node-state node state objs))))

(defn activity-started? [act]
  (= :dispatched (check-activity-state act @state)))

(defn- activity-finished? [act state]
  (= :finished (check-activity-state act state)))

; TODO Add to some protocol
(defn activity-finished [act objs m]
  "To be called when the activity has finished its processing"
  (let [time (pt-timer/getTimeInSeconds m)
        m    (conj m {:time time})]
    (update-dispatch-state! (:uid act) :end-time time)
    #_(println "act finished" (:uid act))
    (let [end-node-id (:end-node act)
          end-node    (end-node-id objs)]
      (when (node-reached? end-node @state objs)
        (dispatch-object end-node objs m)))))

(defn activity-do-not-wait
  "Pretends as if the activity has reached and dispatches next set of objects
  Return value of (dispatch-object) or nil if the activity is not active.
  i.e activity state is #{:not-dispatched :finished}"
  [act objs m]
  (let [act-state (check-activity-state act @state)]
    (if-not (#{:not-dispatched :finished} act-state)
      (let [time        (pt-timer/getTimeInSeconds m)
            m           (conj m {:time time})
            end-node-id (:end-node act)
            end-node    (end-node-id objs)
            new-state   (assoc-in @state [(:uid act) :end-time] time)]
        (when (node-reached? end-node new-state objs)
          (dispatch-object end-node objs m)))
      (println "No action for activity-do-not-wait" (:uid act) act-state))))

(defn- dispatch-activities [act-ids objs m]
  #_(println "Dispatching activities:" act-ids)
  (if (empty? act-ids)
    {}                                                      ;(dispatch-object ((first act-ids) objs) objs m)
    (conj (dispatch-object ((first act-ids) objs) objs m)
          (dispatch-activities (rest act-ids) objs m))))

(defn- dispatch-object-state [node objs m]
  "Helper function to dispatch all the activities of the node
  Returns the list of activities dispatched."
  #_(println "dispatch-object-state" (:uid node) (:tpn-type node))
  (conj {(:uid node) {:uid (:uid node) :tpn-object-state :reached}}
        (let [time (pt-timer/getTimeInSeconds m)
              m    (conj m (:time time))]
          (if (node-dispatched? node @state objs)
            (util/debug-object "Already dispatched node. Not dispatching" node dispatch-object)
            (do
              (update-dispatch-state! (:uid node) :start-time time)
              #_((:dispatch-listener m) node :reached)
              (dispatch-activities (:activities node) objs m))))))

; Dispatch methods
(defmulti dispatch-object
          "Generic function to dispatch the obj
          objs is a map of objects in the tpn index by :uid
          m is map to contain additional information such as
          :activity-dispatcher -- The function that actually does something
          :choice-function -- The function to decide which activity should be dispatched for the choice node"
          (fn [obj _ m]
            ;(println "dispatch-object" (:uid obj) (:tpn-type obj) (:time m))
            (:tpn-type obj)))

(defmethod dispatch-object :default [obj _ _]
  (util/debug-object "dispatch-object :default" obj dispatch-object)
  #_(clojure.pprint/pprint obj)
  {(:uid obj) {:uid (:uid obj) :tpn-object-state :unknown}})

(defmethod dispatch-object :p-begin [obj objs m]
  #_(println "p-begin" (:uid obj) (:tpn-type obj) "-----------")
  (dispatch-object-state obj objs m))

(defmethod dispatch-object :p-end [obj objs m]
  #_(println "p-end" (:uid obj) (:tpn-type obj) "-----------")
  (dispatch-object-state obj objs m))

(defmethod dispatch-object :c-begin [obj objs m]
  #_(println "c-begin" (:uid obj) (:tpn-type obj) "-----------")

  (if dispatch-all-choices
    (dispatch-object-state obj objs m)
    (let [;choice-fn (:choice-fn m)
          ;choice-act-id (choice-fn (:activities obj) m)
          ;choice-act (choice-act-id objs)
          time             (pt-timer/getTimeInSeconds m)
          m                (conj m {:time time})
          first-choice-act (util/get-object (first-choice (:activities obj) nil) objs)
          bindings         (:tpn-bindings @state)
          #_(pprint @state)
          #_(do (println "Bindings")
                (pprint bindings))
          choice-act       (if bindings
                             (util/get-object (get-choice-binding
                                                (:uid obj)
                                                (get-in bindings [(:uid obj) :to-node])
                                                objs)
                                              objs)
                             (do (println "Bindings from planner not available. Using first choice")
                                 first-choice-act))
          ]
      ;(println "choice-act" choice-act )
      (update-dispatch-state! (:uid obj) :start-time time)
      (conj {(:uid obj) {:uid (:uid obj) :tpn-object-state :reached}}
            (dispatch-object choice-act objs m)))))

(defmethod dispatch-object :c-end [obj objs m]
  #_(println "c-end" (:uid obj) (:tpn-type obj) "-----------")
  (dispatch-object-state obj objs m))

(defmethod dispatch-object :state [obj objs m]
  ;(println "dispatch-object state" (:uid obj) (:tpn-type obj) "-----------")
  (dispatch-object-state obj objs m))

(defmethod dispatch-object :activity [obj objs m]
  (let [act-state (check-activity-state obj @state)
        time      (pt-timer/getTimeInSeconds m)
        m         (conj m {:time time})]
    (cond (= :not-dispatched act-state)
          (do
            (update-dispatch-state! (:uid obj) :start-time time)
            {(:uid obj) {:uid (:uid obj) :tpn-object-state :negotiation}})
          (= :cancel act-state)
          (do
            (update-dispatch-state! (:uid obj) :cancelled-time time)
            (conj {(:uid obj) {:uid (:uid obj) :tpn-object-state :cancelled}}
                  (activity-finished obj objs m)))
          :else
          (do
            (util/to-std-err
              (println "dispatch-object :activity unknown activity state" act-state))
            {}))))

(defmethod dispatch-object :null-activity [obj objs m]
  {:post [(map? %)]}
  #_(println "null-activity" (:uid obj) (:tpn-type obj) "-----------")
  (let [time      (pt-timer/getTimeInSeconds m)
        m         (conj m {:time time})
        act-state (check-activity-state obj @state)]
    #_(println "dispatch-object null-activity" (:uid obj) (check-activity-state obj @state))
    (cond (= :not-dispatched act-state)
          (do
            (update-dispatch-state! (:uid obj) :start-time time)
            (conj {(:uid obj) {:uid (:uid obj) :tpn-object-state :finished}}
                  (activity-finished obj objs m)))
          :else {}
          )))

(defmethod dispatch-object :delay-activity [obj _ m]
  (let [time (pt-timer/getTimeInSeconds m)
        m    (conj m {:time time})]
    (update-dispatch-state! (:uid obj) :start-time time))
  #_(println "dispatch-object :delay-activity" obj)
  {(:uid obj) {:uid (:uid obj) :tpn-object-state :started}})

; Dispatch network / dispatch-object should return state of the object
(defmethod dispatch-object :network [obj objs m]
  "Entry point to dispatching the network"
  (update-dispatch-state! (:uid obj) :state :started)
  (let [;a-dispatcher (or (:activity-dispatcher m) simple-activity-dispatcher)
        choice-fn (or (:choice-fn m) first-choice)
        me        (merge m {;:activity-dispatcher a-dispatcher
                            :choice-fn choice-fn})
        begin-obj ((:begin-node obj) objs)
        time      (pt-timer/getTimeInSeconds me)
        me        (conj me {:time time})]
    (println "dispatching begin node")

    (dispatch-object begin-obj objs me)))

(defn- print-node-run-state [val]
  (if (first val) (print "Reached")
                  (print "Not Reached"))
  (println " Pending" (second val) "Start time" (nth val 2)))

(defn- print-activiy-run-state [val]
  (if val (println val)
          (println "Not dispatched")))

; Need object inheritance?
(defn- print-state [ids state objects]
  (doseq [id ids]
    (println "Object" id (get-in objects [id :tpn-type]))
    (cond (contains? #{:p-begin :p-end :c-begin :c-end :state} (get-in objects [id :tpn-type]))
          (print-node-run-state (check-node-state (id objects) state objects))

          (contains? #{:activity :null-activity} (get-in objects [id :tpn-type]))
          (print-activiy-run-state (check-activity-state (id objects) state))
          (= :network (get-in objects [id :tpn-type]))
          (println (id state)))
    (println)))

;;; helpful functions used in conjunction with constraint solver.

(defn print-state-internal [tpn]
  "print internal state of @state"
  (print-state (util/collect-tpn-ids tpn) @state tpn))

(defn- object-reached? [uid tpn-map]
  (let [obj           (uid tpn-map)
        known-object? (and (contains? @state uid)
                           (or (contains? tpn_types/nodetypes (:tpn-type obj))
                               (contains? tpn_types/edgetypes (:tpn-type obj))))]
    (cond known-object?
          (cond (contains? tpn_types/nodetypes (:tpn-type obj))
                (let [value (get-in @state [uid :start-time])]
                  [(not (nil? value)) uid value])

                (contains? tpn_types/edgetypes (:tpn-type obj))
                (let [value (get-in @state [uid :end-time])]
                  [(not (nil? value)) uid value]))

          :else
          (do
            (println "object-reached?: unknown object" uid)
            [false uid]))))

(defn- get-reached-objects [tpn-map]
  (let [reached (reduce (fn [result uid]
                          (conj result (object-reached? uid tpn-map))
                          ) [] (keys @state))]
    (reduce (fn [result triple]
              ;(println triple)
              (if (true? (first triple))
                (merge result {(second triple) (nth triple 2)})
                result
                )) {} reached)))

; TODO Add to some protocol
(defn get-activity-execution-time [uid]
  (let [start-time (get-in @state [uid :start-time])
        end-time   (get-in @state [uid :end-time])]
    (if-not (and start-time end-time)
      (do (println "tpn.dispatch/state has nil time(s)" uid start-time end-time)
          (or end-time start-time))
      (double (- end-time start-time)))))

; TODO Add to some protocol
(defn get-dispatched-activities [tpn-map]
  (let [mystate    @state
        activities (filter (fn [[k act]]
                             (and (contains? tpn_types/edgetypes (:tpn-type act))
                                  (= :dispatched (check-activity-state act mystate))))
                           tpn-map)]
    ;(println "Dispatched activities")
    ;(pprint activities)
    (vals activities)))

; TODO Add to some protocol
(defn get-unfinished-activities [tpn-map]
  (let [activities (filter (fn [[k act]]
                             (and (contains? tpn_types/edgetypes (:tpn-type act))
                                  (not= :finished (check-activity-state act @state))))
                           tpn-map)]
    ;(println "Unfinished activities")
    ;(pprint activities)
    (vals activities)))

; TODO Add to some protocol
(defn get-activities-state
  "Return state of the activities"
  [tpn-map]
  (let [activities (filter (fn [[uid act-obj]]
                             (contains? tpn_types/edgetypes (:tpn-type act-obj)))
                           tpn-map)
        act-state  (reduce (fn [res [uid act-obj]]
                             (merge res {uid (check-activity-state act-obj @state)})) {} activities)]
    act-state))

(defn derive-node-state
  "Here we only know if a node is :reached or :normal"
  [node state tpn]
  (let [node-state (check-node-state node state tpn)
        reached?   (first node-state)]
    {(:uid node) (if reached? :reached :normal)}))

(defn derive-activity-state
  "Here we know a little more about activity state. See check-activity-state"
  [act state]
  (let [act-state (check-activity-state act state)
        act-state (if (= :not-dispatched act-state)
                    :normal
                    act-state)]
    {(:uid act) act-state}))

(defn derive-obj-state [obj state tpn]
  {:post (map? %)}
  (cond (contains? tpn_types/nodetypes (:tpn-type obj))
        (derive-node-state obj state tpn)

        (contains? tpn_types/edgetypes (:tpn-type obj))
        (derive-activity-state obj state)
        :else
        (do
          (util/to-std-err (println "derive-obj-state unkown tpn-type for obj" obj))
          {})))

#_(defn get-tpn-state [tpn]                                 ; not used ; TODO Add to some protocol
    (let [objs (get-nodes-or-activities tpn)]
      (reduce (fn [res obj]
                (conj res (derive-obj-state obj @state tpn))
                ) {} (vals objs))))

(defn all-activities-finished-or-failed? [state tpn]
  (let [acts      (filter (fn [[uid act-obj]]
                            (contains? tpn_types/edgetypes (:tpn-type act-obj)))
                          tpn)
        act-state (select-keys state (keys acts))]
    ;(println "all-activities-finished-or-failed?" act-state)
    ;(pprint act-state)
    ;(def my-acts (into #{} (keys acts)))
    ;(def my-acts-state (into #{} (keys act-state)))
    ;(def my-act-state act-state)
    (and (empty? (set/difference (into #{} (keys acts)) (into #{} (keys act-state))))
         (every? (fn [a-state]
                   (or (not (nil? (get a-state :fail-time nil)))
                       (not (nil? (get a-state :end-time nil)))))
                 (vals act-state)))))

; TODO Add to some protocol
(defn network-finished? [tpn-net]
  ;(println "-- network-finished? " (:network-id tpn-net))
  ;(pprint tpn-net)
  (let [network (util/get-object (:network-id tpn-net) tpn-net)
        begin   (util/get-object (:begin-node network) tpn-net)
        ;end (util/get-object (:end-node begin) tpn-net)
        end     (util/get-network-end-node tpn-net begin)]

    ;(clojure.pprint/pprint network)
    ;(clojure.pprint/pprint begin)
    ;(clojure.pprint/pprint end)

    (and (node-reached? end @state tpn-net)
         #_(all-activities-finished-or-failed? @state tpn-net) ;does not work for choice nodes.
         )))

(defn collect-pending-finished [node state tpn]
  (reduce (fn [res incoming-act]
            (let [act-state (check-activity-state (util/get-object incoming-act tpn) state)]
              ;(println incoming-act act-state)
              (if (or (= :finished act-state)
                      (= :cancelled act-state))
                (update-in res [:finished] conj incoming-act)
                (update-in res [:pending] conj incoming-act))))
          {:finished [] :pending []} (:incidence-set node)))

(defn collect-failed [uids state]
  (reduce (fn [res uid]
            (let [ftime (get-in state [uid :fail-time])]
              ;(println "failed" uid ftime)
              (if (nil? ftime)
                res
                (conj res uid))))
          #{} uids))

(defn make-fail-walker [tpn fail-time]
  " All edges are assumed to be failed.
    If a node is determined to be not-failed,
    then it won't be added to `accumulated` list and will return empty list for `next-objects`
    to stop further failure processing."
  (fn [uid]
    ;(println "fail walker" uid)
    (let [obj (util/get-object uid tpn)
          typ (:tpn-type obj)]

      (cond (util/is-edge obj)
            (do
              (failed-objects [uid] fail-time)
              [[uid] [(:end-node obj)]])


            ; only when we have dispatched all choices, then all of them need to be failed for the choice
            ; node to be failed.
            (and dispatch-all-choices (= :c-end typ))
            (let [incoming-acts    (:incidence-set obj)
                  failed           (collect-failed (:incidence-set obj) @state)
                  pending-finished (collect-pending-finished obj @state tpn)
                  {pending :pending finished :finished} pending-finished
                  node-failed?     (= failed (:incidence-set obj))]
              ;(pprint obj)
              ;(println "incoming acts" incoming-acts)
              ;(pprint (select-keys @state incoming-acts))
              ;(println "pending or finished")
              ;(pprint pending-finished)
              ;(println "failed" failed)
              (if node-failed?
                (do (failed-objects [uid] fail-time)
                    [[uid] (into [] (:activities obj))])
                [[] []]))

            :else
            ; all other nodes are failed
            (do
              (failed-objects [uid] fail-time)
              [[uid] (into [] (:activities obj))])
            ))))

(defn- derive-failed-objects
  "When a activity is failed, corresponding end-node is failed.
   all not-dispatched activities and their end nodes are failed.
   "
  ; TODO When dispatching multiple choices, if all choices are failed then only the choice node is failed
  ; if a choice-end node is failed, then only corresponding forward path is failed.
  [tpn failed-act-id]
  {:pre [(map? tpn)]}
  (let [undispatched (into {} (filter (fn [[_ state]]
                                        (= :not-dispatched state))
                                      (get-activities-state tpn)))
        failed       (into #{failed-act-id} (keys undispatched))

        failed       (into failed (map (fn [act-id]
                                         (:end-node (util/get-object act-id tpn))) failed))]
    failed))

(defn print-time-data [state]
  (pprint state)
  (doseq [[uid obj] state]
    (let [time-vals (select-keys obj #{:start-time :end-time :fail-time})]
      (when (and time-vals (pos? (count time-vals)))
        (println uid ":\n\tstart-time" (str (pt-timer/make-instant-seconds (:start-time obj))))
        (println "\tend-time" (str (pt-timer/make-instant-seconds (:end-time obj))))
        (println "\tfail-time" (str (pt-timer/make-instant-seconds (:fail-time obj))))))
    ))

(defn get-fail-reason
  "For activities that have failed, return the act-id and reason for failure"
  [tpn]
  (let [acts             (reduce (fn [res [act-id data]]
                                   (let [reason (get data :fail-reason)]
                                     (if reason (conj res {act-id reason})
                                                res)))
                                 {} @state)
        choice-end-nodes (remove nil? (map (fn [uid]
                                             (let [obj (util/get-object uid tpn)]
                                               (when (and obj (= :c-end (:tpn-type obj)))
                                                 obj)))
                                           (keys @state)))
        ;_                (pprint choice-end-nodes)
        ;_                (pprint acts)
        node-state       (reduce (fn [res node-obj]
                                   (let [node-acts (:incidence-set node-obj)
                                         act-state (select-keys @state node-acts)]
                                     #_(do (println "node-acts" node-acts)
                                         (println "act-state" act-state))
                                     (conj res (if (and dispatch-all-choices (= node-acts (into #{} (keys act-state))))
                                                 {(:uid node-obj) :choice-all-activities-failed}
                                                 {(:uid node-obj) :choice-some-activities-failed}))))
                                 {} choice-end-nodes)]
    (merge acts node-state)))

(defn activity-failed [failed-act-id tpn fail-time reason]
  ;(println failed-act-id fail-time reason)
  (let [act-state (check-activity-state (util/get-object failed-act-id tpn) @state)]
    (if (= act-state :dispatched)
      (do
        ; update fail reason only for the activity
        (update-state [failed-act-id :fail-reason] reason)
        (util/walker failed-act-id (make-fail-walker tpn fail-time)))
      (do (println "Failed activity" failed-act-id "state is " act-state)))))
