;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns tpn.test-support
  (:require
    [pamela.tools.dispatcher.tpn-walk :as twalk]
    [pamela.tools.utils.tpn-json :as tpn-json]
    [pamela.tools.utils.util :as util]
    ;[pamela.tools.dispatcher.tpnrecords :as trecords]
    [me.raynes.fs :as fs]
    [clojure.pprint :refer :all]
    [clojure.test :refer :all]
    [clojure.java.io]
    ))

(defn checkinstance [c obj]
  ;(println "class " c)
  ;(println "object" obj)
  ;(println "(instance? class obj)" (instance? c obj))
  (= (.getName c) (.getName (type obj)))
  )

(defn check-null-activities [acts objects]
  (is (every? (fn [act]
                (let [obj   (act objects)
                      enode ((:end-node obj) objects)]

                  (= (type obj) pamela.tools.dispatcher.tpnrecords.null-activity)

                  (when (:tc-lb obj)
                    (is (= 0 (:tc-lb obj))))

                  (when (:tc-ub obj)
                    (is (= 0 (:tc-ub obj))))

                  (is (not= nil enode)))
                ) acts)))

(defn check-begin [node begin-type end-type objects]
  (let [e-node ((:end-node node) objects)]
    (is (checkinstance begin-type node))
    (is (checkinstance end-type e-node))
    (check-null-activities (:activities node) objects)
    (check-null-activities (:incidence-set node) objects)))

(defn check-end [node objects]
  (check-null-activities (:activities node) objects)
  (check-null-activities (:incidence-set node) objects))

; TPN Walk test. Ensure all objects are visited once
(defn simple-walk [begin-node-uid objects]
  (with-local-vars [visit-count {}]
    (twalk/begin-walk (begin-node-uid objects) (fn [obj _]
                                                 #_(println "got " (:uid obj) (type obj))
                                                 (var-set visit-count
                                                          (assoc @visit-count
                                                            (:uid obj) (conj ((:uid obj) @visit-count) (:uid obj))))
                                                 #_(println @visit-count)
                                                 ) objects)
    #_(clojure.pprint/pprint @visit-count)
    (is (every? (fn [[_ v]]
                  (= 1 (count v))
                  ) @visit-count))))

(defn read-and-filter-tpns
  "Given a list of json files where each element is of type java.io.File"
  [js-files]
  (remove nil? (map (fn [file-obj]
                      ;(println file-obj)
                      ;(println (fs/normalized file-obj) )
                      ;(println (fs/name file-obj))

                      ;(println (.getCanonicalPath file-obj))

                      (let [tpn-or-htn (tpn-json/from-file file-obj)
                            net-obj    (util/get-network-object tpn-or-htn)]
                        (if (= :network (:tpn-type net-obj))
                          (.getCanonicalPath file-obj)
                          #_{:file file-obj :tpn-map tpn-or-htn}))) js-files)))

(defn get-all-tpn-files-in-test []
  (let [js-files (fs/find-files (str fs/*cwd* "/test") #".*json")
        tpns     (sort (read-and-filter-tpns js-files))
        ; make ready for testing
        tpns     (reduce (fn [result tpn]
                           (conj result [tpn])) [] tpns)
        ]
    (println "Got htn-or-tpn files" (count js-files) "tpn files" (count tpns))
    tpns))

(defn get-tpn-files-by-ext []
  (let [js-files (sort (map (fn [file-obj]
                              (str file-obj))
                            ;Matches *.tpn.json
                            (fs/find-files (str fs/*cwd* "/test") #".*\.tpn\.json")))]
    ;(println "Got tpn files" (count js-files))
    js-files))

(defn get-tpn-bindings-files-in-test []
  (let [files (get-tpn-files-by-ext)]
    (map (fn [tpn-file]
           [tpn-file (str tpn-file ".bindings.json")])
         files)))

(defn get-all-tpn-files-as-vec []
  (into [] (get-all-tpn-files-in-test)))

(defn check-stdout-to-file []
  (with-open [fw (clojure.java.io/writer "temp.txt")]
    (binding [*out* fw]
      (println "Test writing this to file from println/stdout"))))