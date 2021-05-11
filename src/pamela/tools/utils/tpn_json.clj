;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.utils.tpn-json
  "Functions to read TPN JSON data into clojure data structures and
  write json"
  (:require [clojure.data.json :as cl-json]
            [pamela.tools.utils.util :as pt-utils]))

(def debug nil)

(defn to-number-or-keyword [str]
  "Fn to convert stringified number to value or keyword"
  (try
    (Double/parseDouble str)
    (catch NumberFormatException _
      #_(println "converting to str" str)
      (keyword (.toLowerCase str)))))

(defn val-converter [k v]
  "Fn to convert values to appropriate types"
  (cond (contains? #{:nodeid :edgeid :fromid :toid :tpn-type :network-id :uid :end-node :begin-node :tpn-object-state
                     :sequence-end :state :id :finish-state} k)
        (if v (keyword (.toLowerCase (str v)))
              (when debug (println "val-converter: for key" k "value is nil")))

        (contains? #{:activities :incidence-set :constraints} k) (into #{} (map keyword v))

        (= :tc-lb k) (if (= String (type v))
                       (to-number-or-keyword v)
                       v)
        (= :tc-ub k) (if (= String (type v))
                       (to-number-or-keyword v)
                       v)

        (= :cost k) (if (= String (type v))
                      (to-number-or-keyword v)
                      v)

        (= :reward k) (if (= String (type v))
                        (to-number-or-keyword v)
                        v)

        ; Convert other keys as needed. For now they will be string.
        :otherwise v))

(defn map-from-json-str [content]
  (try
    (cl-json/read-str content
                      :key-fn #(keyword %)
                      :value-fn val-converter)
    (catch Exception e
      (println "Error parsing map-from-json-str:\n" content + "\n"))))

(defn from-file [a-file]
  "Read json from a file. a-file could be a string or java.io.File object.
  See https://clojuredocs.org/clojure.java.io/reader"
  (with-open [rdr (clojure.java.io/reader a-file)]
    (cl-json/read rdr
                  :key-fn #(keyword %)
                  :value-fn val-converter)))

(defn to-file [m fname]
  "Write json to the file"
  (with-open [x (clojure.java.io/writer fname)]
    (binding [*out* x]
      (cl-json/pprint m))))

(defn read-bindings-from-json [file]
  (pt-utils/convert-json-bindings-to-clj
    (from-file file)))

(defn write-bindings-to-json [bindings file]
  (to-file (pt-utils/convert-bindings-to-json bindings) file))

(defn read-tpn-with-bindings [tpn-file binding-file]
  {:tpn (if tpn-file
          (from-file tpn-file))
   :bindings (if binding-file
               (read-bindings-from-json binding-file))})


