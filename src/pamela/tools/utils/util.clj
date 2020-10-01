;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.utils.util
  "Set of util functions used everywhere."
  (:import (java.io File)
           (clojure.lang PersistentQueue)
           (java.util.regex Pattern))
  (:require [pamela.tools.utils.tpn-types :as tpn_types]
    ; Only carefully add pamela.tools deps here.
            [clojure.pprint :refer :all]
            [clojure.string :as str]
            [clojure.data :refer :all]
            [clojure.set :as set]
            [clojure.java.io :refer :all]
            [clojure.walk :as w]
            [clojure.data.json :as json]
            [clojure.java.io :as io]))

(def debug nil)

(defmacro to-std-err [& body]
  `(do
     (binding [*out* *err*]
       ~@body)))

(defn as-str-fn [fn]
  "Returns clojure function object as string. Ex: Return value will be tpn.dispatch/dispatch-network"
  (str/replace (first (str/split (str fn) #"@")) #"\$" "/"))

(defn err-println
  "Prints msg and pretty prints obj to stderr."
  ([msg obj]
   (binding [*out* *err*]
     (println msg)
     (when obj (pprint obj))))
  ([msg]
   (err-println msg nil)))

(defn debug-object [msg obj fn]
  "Helper function is used to print where the issue was caught along with message
  and the object that caused the issue"
  (err-println (str (as-str-fn fn) " " msg) obj))

(defn show-updates [old-m new-m]
  "Helper function to show the changes between old and new map.
  Shows new keys and value changes for shared keys"
  (let [[in-old in-new _] (diff old-m new-m)
        updates (set/intersection (set (keys in-old)) (set (keys in-new)))
        new     (set/difference (set (keys in-new)) (set (keys in-old)))]

    (when-not (empty? new)
      (println "New keys for uid" (:uid old-m))
      (doseq [k new]
        (println k "=" (k new-m))))

    (when-not (empty? updates)
      (println "Updates for uid" (:uid old-m) " ## new-val <- old-val")
      (doseq [k updates]
        (println k "=" (k new-m) "<-" (k old-m))))))

; State update helper fns
(defn add-kv-to-objects! [k v objects]
  "Updates objects with k v. Ensures key is not nil and assumes objects is gaurded with atom"
  (if-not k (debug-object "nil key. Not adding to map." v add-kv-to-objects!)
            (swap! objects assoc k v)))

(defn remove-keys [k-set objects]
  (swap! objects (fn [derfd]
                   (apply dissoc derfd k-set))))

(defn get-updated-object [m objects]
  "Returns the object(m) if it does not exists, otherwise the merged object"
  (if-not ((:uid m) objects)
    m
    (merge ((:uid m) objects) m)))

(defn update-object! [m objects]
  "Updates the object with new map. expects (:uid m) is not nil"
  (when-not (:uid m) (debug-object "uid is nill" m update-object!))
  (let [derfd @objects]
    (when (:uid m)
      (when (and debug ((:uid m) derfd))
        (show-updates ((:uid m) derfd) m))
      (add-kv-to-objects! (:uid m) (get-updated-object m derfd) objects))))

(defn make-uid [prefix]
  (keyword (gensym prefix)))

(defn get-object
  "Helper function to work with HTN and TPN data model"
  [uid m]
  "Given a valid uid return the object"
  (when (and debug (not uid))
    (err-println (str "uid is nil " (as-str-fn get-object)))
    (try (throw (Exception. "")) (catch Exception e (.printStackTrace e *out*))))
  (when uid (uid m)))

(defn get-network-object "Assumes m is flat HTN or TPN data model"
  [m]
  (get-object (:network-id m) m))

(defn get-activities "Given TPN, return :activities for the node identified by nid"
  [nid m]
  (:activities (get-object nid m)))

(defn get-end-node-activity "Given activity object and TPN, return end-node-object of the activity"
  [act m]
  (get-object (:end-node act) m))

(defn get-end-node [obj m]
  "Get end node for the object or return nil
   Begin nodes of choice, parallel and sequence should have end nodes
   state nodes of a single activity(non sequence will not have end nodes)"
  (let [has-end-node (:end-node obj)]
    #_(println (:uid obj) has-end-node)
    ; state nodes and c/p-end nodes do not have :end-node ;TODO Fix warnings for state and *-end nodes.
    (cond has-end-node
          (get-object (:end-node obj) m)

          (= :state (:tpn-type obj))
          (get-end-node-activity (get-object (first (:activities obj)) m) m)

          (contains? pamela.tools.utils.tpn-types/edgetypes (:tpn-type obj))
          (do
            (to-std-err (println "FIXME; Where is this called from"))
            (get-end-node-activity (get-object (first (:activities obj)) m) m)) ;FIXME for edge types, we should not have to (:activities obj))


          :otherwise
          (binding [*out* *err*]
            (println "Warning get-end-node for obj is nil" obj)
            nil))))

(defn get-begin-node [tpn]
  (-> tpn :network-id (get-object tpn) :begin-node (get-object tpn)))

(defn has-activities? [node]
  (pos? (count (:activities node))))

(defn get-network-end-node [tpn-net current-node]
  ;(println "get-network-end-node for node" (:uid current-node))
  (if-not (has-activities? current-node)
    current-node
    (let [end (if (:end-node current-node)
                (get-object (:end-node current-node) tpn-net))

          end (if-not end
                (let [act-id (first (:activities current-node))
                      ;_ (println "activities" (:activities current-node))
                      act    (get-object act-id tpn-net)]
                  (get-object (:end-node act) tpn-net))
                end)]
      (get-network-end-node tpn-net end))))

(defn print-persistent-queue [q]
  (print "[")
  (doseq [x q]
    (print x " "))
  (print "]"))

(defn get-constraints [act-id m]
  (let [act         (get-object act-id m)
        constraints (map (fn [cid]
                           (get-object cid m))
                         (:constraints act))]
    constraints))

(defn get-activity-temporal-constraint-value
  "Assumes activity can have only one temporal constraint"
  [act-id m]
  (let [constrains (get-constraints act-id m)]
    (if (first constrains)
      (:value (first constrains))
      [0 Double/POSITIVE_INFINITY])))

(defn get-all-activities
  "Starting at given node, return all activities until end-node of node-obj is found"
  [node-obj m]

  (when-not (get-end-node node-obj m)
    (to-std-err
      (println "Error: get-all-activities Search for end-node yielded nil")
      (pprint node-obj)))

  (loop [end            (get-end-node node-obj m)
         nodes-to-visit (conj (PersistentQueue/EMPTY) node-obj)
         activities     #{}]

    (if (empty? nodes-to-visit)
      activities
      (let [n             (first nodes-to-visit)
            acts          (if (not= (:uid n) (:uid end))
                            (:activities n)
                            #{})
            act-end-nodes (reduce (fn [result act-id]
                                    (conj result (get-end-node (get-object act-id m) m)))
                                  [] acts)]
        ;(println "Nodes to visit"  )
        ;(pprint (into [] nodes-to-visit))
        ;(println "Node" )
        ;(pprint n)
        ;(println "activities" acts)
        ;(println "act end nodes" )
        ;(pprint act-end-nodes)
        ;(println "END node" end)
        ;(println "-----------")
        (recur end (into (pop nodes-to-visit) act-end-nodes) (into activities acts))))))

(defn find-any-path
  "Starting with start-uid, walk the graph until end-uid is found.
  Return vector containing all the object-uids found in traversal, including start-uid and end-uid.
  The vector will have the object-uids in the order they are visited.
  If the end-uid is not found, last element will be nil."
  [start-uid end-uid m]
  (loop [result         []
         current-object (get-object start-uid m)]
    (if (or (= end-uid (:uid current-object))
            (nil? current-object))
      ; The last element of the result will be nil if search for end-uid fails.
      (conj result (:uid current-object))
      (recur (conj result (:uid current-object))
             (get-object (or (first (:activities current-object))
                             (:end-node current-object))
                         m)))))

(defn map-from-json-str [jsn]
  (try
    (json/read-str jsn :key-fn #(keyword %))
    (catch Exception _
      (to-std-err
        (println "Error parsing map-from-json-str:\n" jsn + "\n")))))

;;; Function to read rmq-logger generated CSV line containing timestamp and json message
;;; Return time-stamp as ? and json message as clj-map
(defn parse-rmq-logger-json-line [line]
  (let [time-begin (inc (str/index-of line ","))
        time       (.substring line time-begin)
        time-end   (+ time-begin (str/index-of time ","))

        ts         (str/trim (.substring line time-begin time-end))
        data       (str/trim (.substring line (inc time-end)))]


    {:recv-ts (read-string ts)
     :data    (map-from-json-str data)}))

(defn read-lines [fname]
  "Read lines from a file"
  (if (.exists (io/as-file fname))
    (with-open [r (reader fname)]
      (doall (line-seq r)))
    (do (println "file does not exist" fname))))

(defn map-invert
  "Convert map of keys and values to {val #{keys}" [m]
  (reduce (fn [result [k v]]
            (if-not (contains? result v)
              (conj result {v #{k}})
              (update result v #(conj % k))))
          {} m))

(defn check-type [c obj]
  (if-not (instance? c obj)
    (to-std-err (println "check-type: expected, c = " (.getName c) "got, obj = " (type obj)))
    true))

(defn getCurrentThreadName []
  (.getName (Thread/currentThread)))

#_(def my-libs-info #{"tpn." "repr."})
(defn my-libs []
  (filter (fn [lib]
            ;(println "lib" lib)
            ;(println "type" (type lib))
            (or (str/starts-with? (str lib) "tpn.")
                (str/starts-with? (str lib) "repr.")))
          (loaded-libs)))

(defn remove-namespaces [spaces]
  (doseq [sp spaces]
    (remove-ns sp)))

(defn dirs-on-classpath []
  (filter #(.isDirectory ^File %)
          (map #(File. ^String %)
               (str/split
                 (System/getProperty "java.class.path")
                 (Pattern/compile (Pattern/quote File/pathSeparator))))))

(defn get-everything-after [nth separator line]
  (loop [sep-count 0
         after     line]
    (if (= nth sep-count)
      after
      (let [index     (str/index-of after separator)
            new-after (when index (.substring after (inc index)))]
        (recur (inc sep-count) new-after)))))

(defn get-nodes-or-activities [tpn]
  (into {} (filter (fn [[_ obj]]
                     (or (contains? tpn_types/nodetypes (:tpn-type obj))
                         (contains? tpn_types/edgetypes (:tpn-type obj))))
                   tpn)))

#_(defn get-temporal-constraints [uid m]
    (let [obj         (get-object uid m)
          constraints (:constraints obj)
          constraints (filter (fn [cid]
                                (= :temporal-constraint (:tpn-type (get-object cid m)))) constraints)]
      (into #{} constraints)))

#_(defn get-uids-with-tc [uids tpn-map]
    (into {} (remove (fn [[_ val]]
                       (zero? (count val)))
                     (reduce (fn [result uid]
                               (merge result {uid (get-temporal-constraints uid tpn-map)}))
                             {} uids))))
; (tpn.util/send-to "localhost" 34170 (slurp "test/data/tpn.json"))

(defn parse-rmq-logger-file [file]
  (let [lines (read-lines file)]
    (map (fn [line]
           (parse-rmq-logger-json-line line)) lines)))

(defn convert-json-bindings-to-clj
  "If any of the element is \"Infinity\" or \"-Infinity\" then convert it to corresponding
  java.lang.double version"
  [bindings]
  (w/postwalk (fn [x]
                (cond (= x (str Double/POSITIVE_INFINITY))
                      Double/POSITIVE_INFINITY
                      (= x (str Double/NEGATIVE_INFINITY))
                      Double/NEGATIVE_INFINITY
                      (and (vector? x) (= :to-node (first x)))
                      [:to-node (keyword (second x))]
                      :else x))
              bindings))

(defn convert-bindings-to-json
  "If any of the element is java.lang.Double/POSITIVE_INFINITY or java.lang.Double/NEGATIVE_INFINITY then convert it to corresponding
  string version"
  [bindings]
  (w/postwalk (fn [x]
                (cond (= x Double/POSITIVE_INFINITY)
                      (str Double/POSITIVE_INFINITY)
                      (= x Double/NEGATIVE_INFINITY)
                      (str Double/NEGATIVE_INFINITY)
                      :else x))
              bindings))

(defn is-node "Return node type"
  [obj]
  ((:tpn-type obj) tpn_types/nodetypes))

(defn is-edge "Return node type"
  [obj]
  ((:tpn-type obj) tpn_types/edgetypes))

(defn make-bfs-walk
  "Return list of collected and next set of objects"
  [tpn]
  (let [handled (atom #{})]
    (fn [uid]
      ;(println uid)
      ;(println "handled" @handled)
      (if-not (contains? @handled uid)
        (let [obj       (get-object uid tpn)
              is_node   ((:tpn-type obj) tpn_types/nodetypes)
              is_edge   ((:tpn-type obj) tpn_types/edgetypes)
              next-objs (cond is_node (:activities obj)
                              is_edge [(:end-node obj)]
                              :else [])]
          ;(def handled (conj handled uid))
          (swap! handled conj uid)
          ;(println "handled 2" @handled )
          [[uid] (into [] next-objs)])))))

(defn walk [obj fn]
  (fn obj))

(defn walker [begin fn]
  "fn must return a vector of two vectors
  first vector should be elements that are `accumulated`
  second vector should be elements that the function has determined for further processing to let
  walker continue walking the graph
  "
  (loop [collected []
         remain    [begin]]
    ;(println "collected" collected)
    ;(println "remain" remain)
    (if (nil? (first remain))
      collected
      (let [[collected-w remain-w] (walk (first remain) fn)]
        ;(println "collected-w" collected-w)
        ;(println "remain-w" remain-w)
        ;(println "-----")
        (recur (into collected collected-w) (into (into [] (rest remain)) remain-w))))))


(defn walk-tpn [tpn fn]
  (walker (:uid (get-begin-node tpn)) fn))

(defn collect-tpn-ids [tpn]
  (walker (:uid (get-begin-node tpn)) (make-bfs-walk tpn)))

(defn filter-activities [tpn]
  (filter (fn [obj]
            (= :activity (:tpn-type obj))) (vals tpn)))

(defn get-plant-ids [tpn]
  (let [ids (into #{} (remove nil? (map (fn [act-obj]
                                          (:plant act-obj)) (filter-activities tpn))))]
    (if (empty? ids)
      #{"plant"} ids)))

(defn rmq-data-to-string [data]
  (String. data "UTF-8"))

(defn rmq-data-to-clj
  "Convert bytes received from RMQ callback to clojure native data structure
  Assumes bytes represent json string"
  [data]
  (map-from-json-str (rmq-data-to-string data)))

(defn copy-next-action [next-action from-id to-id]
  (if (nil? (get next-action from-id))
    (to-std-err (println "copy-next-action for to-id" to-id "from-id" from-id "not found in" next-action)
                next-action)
    (merge next-action {to-id (from-id next-action)})))

(defn- make-next-action [obj next-actions tpn]
  ;(println "obj" obj)
  ;(println "next actions")
  ;(pprint next-actions)
  (cond (nil? obj)
        next-actions

        (contains? next-actions (:uid obj))
        next-actions

        (or (= :null-activity (:tpn-type obj))
            (= :activity (:tpn-type obj))
            (= :delay-activity (:tpn-type obj)))
        (do
          ;(println "got" (:uid obj) (:tpn-type obj))
          (copy-next-action (make-next-action (get-end-node-activity obj tpn) next-actions tpn)
                            (:end-node obj)
                            (:uid obj)))

        (= :state (:tpn-type obj))
        (do
          ;(println "got" (:uid obj) (:tpn-type obj))
          (if (not (empty? (:activities obj)))
            (let [act-id  (first (:activities obj))
                  act-obj (get-object act-id tpn)]

              (if (= :activity (:tpn-type act-obj))         ;state node has activity, so next action is the activity
                (conj next-actions {(:uid obj) [act-obj]})
                ; state node has delay or null activity, so next action is whatever is the next action for null/delay activity
                (copy-next-action (make-next-action act-obj next-actions tpn) act-id (:uid obj))))

            ; state node is end node of the sequence. So no next actions
            (conj next-actions {(:uid obj) []})))

        (or (= :p-begin (:tpn-type obj))
            (= :c-begin (:tpn-type obj)))
        (do
          ;(println "got" (:uid obj) (:tpn-type obj))
          (let [collected-acts (reduce (fn [res act-id]
                                         (conj res (make-next-action (get-object act-id tpn) next-actions tpn)))
                                       next-actions (:activities obj))]
            ;(println "collected acts for node" (:tpn-type obj) (:activities obj))
            ;(pprint collected-acts)
            (conj collected-acts {(:uid obj) (reduce (fn [res act-id]
                                                       (into res (act-id collected-acts)))
                                                     [] (:activities obj))})))

        (or (= :p-end (:tpn-type obj))
            (= :c-end (:tpn-type obj)))
        (do
          ;(println "got" (:uid obj) (:tpn-type obj))
          (if (not (empty? (:activities obj)))
            (copy-next-action (make-next-action (get-object (first (:activities obj)) tpn)
                                                next-actions
                                                tpn)
                              (first (:activities obj)) (:uid obj))
            ; node is the last node of the TPN and hence no next actions
            (conj next-actions {(:uid obj) []})))

        :else
        (to-std-err #_(println "Unknown object")
          #_(pprint obj)
          next-actions)))

(defn make-next-actions
  "For given tpn nodes and activities, create a map of id to a list of activity objects
  The list of activity objects contains list of next dispatchable set of activities.
  i.e it won't have any null activities.
  If the next activity is part of a sequence, then the list will contain 1 activity
  If the next activity is part of choice of parallel node, then the list will contain all
  the activities of the node.
  "
  [tpn]
  (reduce (fn [res act-obj]
            (let [x (make-next-action act-obj res tpn)]
              (conj res x)))
          {} (vals (get-nodes-or-activities tpn))))

(defn get-all-threads
  "fn to return threads sorted by their run time state"
  []
  #_(doseq [th (keys (Thread/getAllStackTraces))]
      (println (.getName th) (.name (.getState th))))
  (group-by (fn [x]
              (second x))
            (sort-by first (map (fn [th]
                                  [(.getName th) (.name (.getState th))])
                                (keys (Thread/getAllStackTraces))))))