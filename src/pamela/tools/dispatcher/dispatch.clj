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
    [pamela.tools.utils.util :refer :all]
    [pamela.tools.utils.tpn-types :as tpn_types]
    [pamela.tools.mct-planner.expr :as expr]
    [pamela.tools.mct-planner.util :as rutil]
    [pamela.tools.mct-planner.solver :as solver]
    [pamela.tools.utils.util :as util]

    [clojure.pprint :refer :all]
    [clojure.set :as set]))

; Forward declarations.
(declare activity-finished?)
(declare dispatch-object)

(defonce state (atom {}))
(defonce tpn-info {})

(defn- update-state
  "uses update-in to update state atom"
  [keys value]
  (if-not value
    (to-std-err (println "Warn: update-state value is nil for keys:" keys)))

  (let [pre-cond (every? #(not (nil? %)) keys)]
    (if-not pre-cond
      (to-std-err (println "Error: update-state at least one of the keys is nil: Won't update, Given keys" keys))
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
    (update-state [obj-id :start-time] time)
    (update-state [obj-id :end-time] time)
    (update-state [obj-id :fail-time] time))
  (update-plant-dispatch-id-internal [this act-id disp-id]
    (update-state [act-id :plant-dispatched-id] disp-id))
  (get-plant-dispatch-id-internal [this act-id]
    (get-in @state [act-id :plant-dispatched-id])))

; Just an object to work with state atom
(def dispatch-state (->dispatchR))

(defn cancel-activities [uids time]
  (let [t (getTimeInSeconds {:time time})]
    (doseq [uid uids]
      (cancel dispatch-state uid t))))

(defn failed-objects [uids time]
  (let [t (getTimeInSeconds {:time time})]
    (doseq [uid uids]
      (failed dispatch-state uid t))))

(defn update-plant-dispatch-id [act-id disp-id]
  (update-plant-dispatch-id-internal dispatch-state act-id disp-id))

(defn get-plant-dispatch-id [act-id]
  (get-plant-dispatch-id-internal dispatch-state act-id))

(defn reset-state []
  (reset! state {}))

(defn set-tpn-info [tpn-map exprs-details]
  (def tpn-info {:tpn-map      tpn-map
                 :expr-details exprs-details}))             ;

(defn- get-tpn []
  (:tpn-map tpn-info))

(defn- get-exprs []
  (get-in tpn-info [:expr-details :all-exprs]))

(defn- get-nid-2-var []
  (get-in tpn-info [:expr-details :nid-2-var]))

;; Solver stuff
(defn- node-object-started? [uid tpn-map]
  (let [obj (uid tpn-map)
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

(defn- get-node-id-2-var []
  (get-in tpn-info [:expr-details :nid-2-var]))

(defn nid-2-var-range
  "Return node-id to range var mapping"
  [nid-2-var]
  (reduce (fn [result [nid vars]]
            (let [range-vars (filter (fn [var]
                                       (rutil/is-range-var? var))
                                     vars)
                  range-var (first range-vars)]

              (if-not (nil? range-var)
                (conj result {nid range-var})
                result)))
          {} nid-2-var))

; compute expr bindings from tpn state
;  * Find objects that have reached, and find their end-time
;  * exprs that refer to any of the reached object will have their to-var set to the end-time
(defn temporal-bindings-from-tpn-state                      ;WORKs with shape of data returned from nid-2-var-range

  [nid-2-var obj-times]
  ;(println "obj-times")
  ;(pprint obj-times)
  ;(println "nid-2-var")
  ;(pprint nid-2-var)
  (reduce (fn [result [uid time]]
            (conj result {(uid nid-2-var) time}))
          {} obj-times))

(defn- run-solver []
  (let [nid2-var (get-node-id-2-var)
        nid-2-var-range-x (nid-2-var-range nid2-var)
        reached-state (get-node-started-times (get-tpn))
        initial-bindings (temporal-bindings-from-tpn-state nid-2-var-range-x reached-state)
        sample (if (pos? (count initial-bindings))
                 (solver/solve (get-exprs) (get-nid-2-var) 1 initial-bindings)
                 (solver/solve (get-exprs) (get-nid-2-var) 1))

        sample (first sample)
        new-bindings (:bindings sample)]
    ;(update-state [:sample] sample)
    ;(let [prev-reached-state (:reached-state @state)
    ;      updated (if prev-reached-state
    ;                (conj prev-reached-state reached-state)
    ;                [reached-state])]
    ;  (update-state [:reached-state] updated))

    ;(collect-bindings initial-bindings new-bindings)

    (when debug
      (println "expr initial bindings")
      (pprint initial-bindings)
      (println "expr new bindings")
      (pprint new-bindings)

      ;(println "normal uids:" normal-tc-ids)
      ;(println "Failed expression ids:" failed-tc-ids)
      (println "Bound expr count: " (count (:expr-values sample)))
      (pprint (:expr-values sample)))
    sample))

(defn- get-choice-var [uid node-vars]
  (first (filter rutil/is-select-var? node-vars)))

(defn- find-activity [src-uid target-uid tpn-map]
  (let [acts (:activities (get-object src-uid tpn-map))
        act-objs (map (fn [act-id]
                        (get-object act-id tpn-map)) acts)
        found (filter (fn [act]
                        (= target-uid (:uid (get-end-node-activity act tpn-map))))
                      act-objs)]
    (first found)))

(defn- get-choice-binding [uid expr-details sample tpn-map]
  ; return chosen activity
  (let [choice-var (get-choice-var uid (get-in expr-details [:nid-2-var uid]))
        ;bindings (:bindings sample)
        bound-var (get-in sample [:bindings choice-var])
        bound-node (get-in expr-details [:var-2-nid bound-var])]
    (find-activity uid bound-node tpn-map)))
;; Solver stuff ends


(defn- reset-state-network [ids]
  "Walk the TPN for the given network and remove all objects from state"
  (remove-keys ids state))                                  ;(walk/collect-tpn-ids netid objects)

(defn- update-dispatch-state! [uid key value]
  "Updates the runtime state of the object identified by uid"
  #_(println "update-state" uid key value)
  (if-not uid
    (debug-object (str "Cannot update run time state for nil uid " key " " value) nil update-dispatch-state!)
    (do (when (get-in @state [uid key])
          (println "Warning tpn.dispatch/state has [uid key]" uid (get-in @state [uid key])))
        (swap! state assoc-in [uid key] value))))

(defn- simple-activity-dispatcher [act _ _]                 ;objs and m
  (println "simple-activity-dispatcher" (:uid act) "type" (:tpn-type act) (:name act)))

;TODO deprecated in favor of solver providing the chosen path
(defn- first-choice [activities _]
  "Return the first activity"
  (if (empty? activities)
    (debug-object "Activity list is empty" activities first-choice)
    (first activities)))

(defn- node-reached-helper [node state objs]                ;TODO Candidate for refactor
  "A node is reached if all the activities in the incidence set are finished."
  #_(println "\nnode-reached-helper" (:uid node))
  (with-local-vars [pending-completion #{}
                    finished #{}]
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
  (let [nid (:uid node)
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
  (let [id (:uid act)
        start-time (get-in state [id :start-time])
        end-time (get-in state [id :end-time])
        cancel-time (get-in state [id :cancel-time])
        cancelled-time (get-in state [id :cancelled-time])]
    #_(println "act start end " id start-time end-time)
    (cond (= nil (id state))
          (do
            ;(println "unknown activity" id)
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
          (do (debug-object "Error in Activity state" act check-activity-state)
              :error))))

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

(defn- activity-started? [act state]
  (= :dispatched (check-activity-state act state)))

(defn- activity-finished? [act state]
  (= :finished (check-activity-state act state)))

; TODO Add to some protocol
(defn activity-finished [act objs m]
  "To be called when the activity has finished its processing"
  (let [time (getTimeInSeconds m)
        m (conj m {:time time})]
    (update-dispatch-state! (:uid act) :end-time time)
    #_(println "act finished" (:uid act))
    (let [end-node-id (:end-node act)
          end-node (end-node-id objs)]
      (when (node-reached? end-node @state objs)
        (dispatch-object end-node objs m)))))

(defn activity-do-not-wait
  "Pretends as if the activity has reached and dispatches next set of objects
  Return value of (dispatch-object) or nil if the activity is not active.
  i.e activity state is #{:not-dispatched :finished}"
  [act objs m]
  (let [act-state (check-activity-state act @state)]
    (if-not (#{:not-dispatched :finished} act-state)
      (let [time (getTimeInSeconds m)
            m (conj m {:time time})
            end-node-id (:end-node act)
            end-node (end-node-id objs)
            new-state (assoc-in @state [(:uid act) :end-time] time)]
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
  (let [time (getTimeInSeconds m)
        m (conj m (:time time))]
    (if (node-dispatched? node @state objs)
      (debug-object "Already dispatched node. Not dispatching" node dispatch-object)
      (do
        (update-dispatch-state! (:uid node) :start-time time)
        #_((:dispatch-listener m) node :reached)
        (dispatch-activities (:activities node) objs m))))
  )

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
  (debug-object "dispatch-object :default" obj dispatch-object)
  #_(clojure.pprint/pprint obj)
  {(:uid obj) {:uid (:uid obj) :tpn-object-state :unkown}})

(defmethod dispatch-object :p-begin [obj objs m]
  #_(println "p-begin" (:uid obj) (:tpn-type obj) "-----------")
  (conj {(:uid obj) {:uid (:uid obj) :tpn-object-state :reached}}
        (dispatch-object-state obj objs m)))

(defmethod dispatch-object :p-end [obj objs m]
  #_(println "p-end" (:uid obj) (:tpn-type obj) "-----------")
  (conj {(:uid obj) {:uid (:uid obj) :tpn-object-state :reached}}
        (dispatch-object-state obj objs m)))

(defmethod dispatch-object :c-begin [obj objs m]
  #_(println "c-begin" (:uid obj) (:tpn-type obj) "-----------")
  (let [;choice-fn (:choice-fn m)
        ;choice-act-id (choice-fn (:activities obj) m)
        ;choice-act (choice-act-id objs)
        time (getTimeInSeconds m)
        m (conj m {:time time})
        sample (run-solver)
        choice-act (get-choice-binding (:uid obj) (:expr-details tpn-info) sample (get-tpn))]
    (update-dispatch-state! (:uid obj) :start-time time)
    (conj {(:uid obj) {:uid (:uid obj) :tpn-object-state :reached}}
          (dispatch-object choice-act objs m))))

(defmethod dispatch-object :c-end [obj objs m]
  #_(println "c-end" (:uid obj) (:tpn-type obj) "-----------")
  (conj {(:uid obj) {:uid (:uid obj) :tpn-object-state :reached}}
        (dispatch-object-state obj objs m)))

(defmethod dispatch-object :state [obj objs m]
  ;(println "dispatch-object state" (:uid obj) (:tpn-type obj) "-----------")
  (conj {(:uid obj) {:uid (:uid obj) :tpn-object-state :reached}}
        (dispatch-object-state obj objs m)))

(defmethod dispatch-object :activity [obj objs m]
  (let [act-state (check-activity-state obj @state)
        time (getTimeInSeconds m)
        m (conj m {:time time})]
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
            (to-std-err
              (println "dispatch-object :activity unknown activity state" act-state))
            {}))))

(defmethod dispatch-object :null-activity [obj objs m]
  {:post [(map? %)]}
  #_(println "null-activity" (:uid obj) (:tpn-type obj) "-----------")
  (let [time (getTimeInSeconds m)
        m (conj m {:time time})
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
  (let [time (getTimeInSeconds m)
        m (conj m {:time time})]
    (update-dispatch-state! (:uid obj) :start-time time))
  #_(println "dispatch-object :delay-activity" obj)
  {(:uid obj) {:uid (:uid obj) :tpn-object-state :started}})

; Dispatch network / dispatch-object should return state of the object
(defmethod dispatch-object :network [obj objs m]
  "Entry point to dispatching the network"
  (update-dispatch-state! (:uid obj) :state :started)
  (let [;a-dispatcher (or (:activity-dispatcher m) simple-activity-dispatcher)
        choice-fn (or (:choice-fn m) first-choice)
        me (merge m {;:activity-dispatcher a-dispatcher
                     :choice-fn choice-fn})
        begin-obj ((:begin-node obj) objs)
        time (getTimeInSeconds me)
        me (conj me {:time time})]
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

(defn- object-reached? [uid tpn-map]
  (let [obj (uid tpn-map)
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
        end-time (get-in @state [uid :end-time])]
    (if-not (and start-time end-time)
      (do (println "tpn.dispatch/state has nil time(s)" uid start-time end-time)
          (or end-time start-time))
      (double (- end-time start-time)))))

; TODO Add to some protocol
(defn get-dispatched-activities [tpn-map]
  (let [mystate @state
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
        act-state (reduce (fn [res [uid act-obj]]
                            (merge res {uid (check-activity-state act-obj @state)})) {} activities)]
    act-state))

(defn derive-node-state
  "Here we only know if a node is :reached or :normal"
  [node state tpn]
  (let [node-state (check-node-state node state tpn)
        reached? (first node-state)]
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

; TODO Add to some protocol
(defn get-tpn-state [tpn]
  (let [objs (get-nodes-or-activities tpn)]
    (reduce (fn [res obj]
              (conj res (derive-obj-state obj @state tpn))
              ) {} (vals objs))))

(defn all-activities-finished-or-failed? [state tpn]
  (let [acts (filter (fn [[uid act-obj]]
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
  (let [network (get-object (:network-id tpn-net) tpn-net)
        begin (get-object (:begin-node network) tpn-net)
        ;end (get-object (:end-node begin) tpn-net)
        end (get-network-end-node tpn-net begin)]

    ;(clojure.pprint/pprint network)
    ;(clojure.pprint/pprint begin)
    ;(clojure.pprint/pprint end)

    (and (node-reached? end @state tpn-net)
         #_(all-activities-finished-or-failed? @state tpn-net) ;does not work for choice nodes.
         )))

(defn derive-failed-objects
  "When a activity is failed, corresponding end-node is failed.
   all not-dispatched activities and their end nodes are failed.
   "

  [tpn failed-act-id]
  {:pre [(map? tpn)]}
  (let [undispatched (into {} (filter (fn [[uid state]]
                                        (= :not-dispatched state))
                                      (get-activities-state tpn)))
        failed (into #{failed-act-id} (keys undispatched))

        failed (into failed (map (fn [act-id]
                                   (:end-node (get-object act-id tpn))) failed))]
    failed))