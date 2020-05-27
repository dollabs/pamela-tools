;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.utils.util
  "Set of util functions used everywhere."
  (:import (java.net Socket)
           (java.io OutputStreamWriter File)
           (clojure.lang PersistentQueue)
           (java.util.regex Pattern))
  (:require [pamela.tools.utils.tpn-types :as tpn_types]

            [clojure.pprint :refer :all]
            [clojure.string :as str]
            [clojure.data :refer :all]
            [clojure.set :as set]
            [clojure.java.io :refer :all]
            [clojure.walk :as w]
            [clojure.data.json :as json]))

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
        new (set/difference (set (keys in-new)) (set (keys in-old)))]

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
                      act (get-object act-id tpn-net)
                      ]
                  (get-object (:end-node act) tpn-net))
                end)]
      (get-network-end-node tpn-net end))))

(defn print-persistent-queue [q]
  (print "[")
  (doseq [x q]
    (print x " "))
  (print "]"))

(defn get-constraints [act-id m]
  (let [act (get-object act-id m)
        constraints (map (fn [cid]
                           (get-object cid m)
                           ) (:constraints act))]
    constraints))

(defn get-activity-temporal-constraint-value
  "Assumes activity can have only one temporal constraint"
  [act-id m]
  (let [constrains (get-constraints act-id m)]
    (if (first constrains)
      (:value (first constrains))
      [0 java.lang.Double/POSITIVE_INFINITY]
      )))

(defn get-all-activities
  "Starting at given node, return all activities until end-node of node-obj is found"
  [node-obj m]

  (when-not (get-end-node node-obj m)
    (to-std-err
      (println "Error: get-all-activities Search for end-node yielded nil")
      (pprint node-obj)))

  (loop [end (get-end-node node-obj m)
         nodes-to-visit (conj (PersistentQueue/EMPTY) node-obj)
         activities #{}]

    (if (empty? nodes-to-visit)
      activities
      (let [n (first nodes-to-visit)
            acts (if (not= (:uid n) (:uid end))
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
        (recur end (into (pop nodes-to-visit) act-end-nodes) (into activities acts))
        ))))

(defn find-any-path
  "Starting with start-uid, walk the graph until end-uid is found.
  Return vector containing all the object-uids found in traversal, including start-uid and end-uid.
  The vector will have the object-uids in the order they are visited.
  If the end-uid is not found, last element will be nil."
  [start-uid end-uid m]
  (loop [result []
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
    (catch Exception e
      (to-std-err
        (println "Error parsing map-from-json-str:\n" jsn + "\n")))))

;;; Function to read rmq-logger generated CSV line containing timestamp and json message
;;; Return time-stamp as ? and json message as clj-map
(defn parse-rmq-logger-json-line [line]
  (let [time-begin (inc (str/index-of line ","))
        time (.substring line time-begin)
        time-end (+ time-begin (str/index-of time ","))

        ts (str/trim (.substring line time-begin time-end))
        data (str/trim (.substring line (inc time-end)))
        ]

    {:recv-ts (read-string ts)
     :data    (map-from-json-str data)}))

(defn read-lines [fname]
  "Read lines from a file"
  (with-open [r (reader fname)]
    (doall (line-seq r))))

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

#(def my-libs-info #{"tpn." "repr."})
(defn my-libs []
  (filter (fn [lib]
            ;(println "lib" lib)
            ;(println "type" (type lib))
            (or (str/starts-with? (str lib) "tpn.")
                (str/starts-with? (str lib) "repr."))
            ) (loaded-libs)))

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
         after line]
    (if (= nth sep-count)
      after
      (let [index (str/index-of after separator)
            new-after (when index (.substring after (inc index)))]
        (recur (inc sep-count) new-after)))))

(defn get-nodes-or-activities [tpn]
  (into {} (filter (fn [[uid obj]]
                     (or (contains? tpn_types/nodetypes (:tpn-type obj))
                         (contains? tpn_types/edgetypes (:tpn-type obj)))
                     ) tpn)))

#_(defn get-temporal-constraints [uid m]
    (let [obj (get-object uid m)
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

(defn infinity-to-string
  "If any of the element is Infinity or -Infinity, stringify it"
  [bindings]
  (w/postwalk (fn [x]
                (cond (= x java.lang.Double/POSITIVE_INFINITY)
                      (str java.lang.Double/POSITIVE_INFINITY)

                      (= x java.lang.Double/NEGATIVE_INFINITY)
                      (str java.lang.Double/NEGATIVE_INFINITY)

                      :else x))
              bindings))

(defn string-to-infinity
  "If any of the element is \"Infinity\" or \"-Infinity\" then convert it to corresponding
  java.lang.double version"
  [bindings]
  (w/postwalk (fn [x]
                (cond (= x (str java.lang.Double/POSITIVE_INFINITY))
                      java.lang.Double/POSITIVE_INFINITY
                      (= x (str java.lang.Double/NEGATIVE_INFINITY))
                      java.lang.Double/NEGATIVE_INFINITY
                      :else x))
              bindings))

(defn is-node "Return node type"
  [obj]
  ((:tpn-type obj) tpn_types/nodetypes))

(defn is-edge "Return node type"
  [obj]
  ((:tpn-type obj) tpn_types/edgetypes))

(defn collect-objects-in-bfs-xx [tpn]
  (let [begin-node (:uid (get-begin-node tpn))]
    (loop [objs []
           handled #{}
           remain [begin-node]]
      (if (nil? (first remain))
        objs
        (let [obj (get-object (first remain) tpn)
              is_node ((:tpn-type obj) tpn_types/nodetypes)
              is_edge ((:tpn-type obj) tpn_types/edgetypes)
              next-objs (cond is_node (:activities obj)
                              is_edge [(:end-node obj)]
                              :else [])]
          (pprint obj)
          (println (:uid obj))
          (println is_node is_edge)
          (println objs)
          (println remain)
          (println "-----")
          (recur (if (contains? handled (:uid obj))
                   objs
                   (conj objs (:uid obj)))
                 (conj handled (:uid obj))
                 (into (into [] (rest remain)) next-objs)))))))

(defn make-bfs-walk
  "Return list of collected and next set of objects"
  [tpn]
  (let [handled (atom #{})]
    (fn [uid]
      ;(println uid)
      ;(println "handled" @handled)
      (if-not (contains? @handled uid)
        (let [obj (get-object uid tpn)
              is_node ((:tpn-type obj) tpn_types/nodetypes)
              is_edge ((:tpn-type obj) tpn_types/edgetypes)
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
         remain [begin]]
    ;(println "collected" collected)
    ;(println "remain" remain)
    (if (nil? (first remain))
      collected
      (let [[collected-w remain-w] (walk (first remain) fn)]
        ;(println "collected-w" collected-w)
        ;(println "remain-w" remain-w)
        ;(println "-----")
        (recur (into collected collected-w) (into (into [] (rest remain)) remain-w))
        ))))

(defn walk-tpn [tpn fn]
  (walker (:uid (get-begin-node tpn)) fn))

(defn collect-tpn-ids [tpn]
  (walker (:uid (get-begin-node tpn)) (make-bfs-walk tpn)))