;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.dispatcher.tpn-import
  "Functions to convert TPN JSON to TPN defrecords"
  (:require [pamela.tools.dispatcher.tpnrecords :as tpn_records]
            [pamela.tools.utils.tpn-json :as tpn-json]
            [clojure.pprint :refer :all]))                  ;:rename {pprint mpp} ;sugar

(def debug true)

(defn make-object [obj create-fn]
  (if create-fn
    (create-fn obj :uid (:uid obj))
    (do
      (when debug (println "tpn.import/make-object create-fn is nil for" (:tpn-type obj)))
      obj)))

(defn from-map [objs]
  (with-local-vars [m {}]
    (doseq [[k v] objs]
      (if (:tpn-type v)
        (var-set m (assoc @m k (make-object v (tpn_records/tpn-type-2-create (:tpn-type v)))))
        (when debug (println "import tpn. No constructor for key" k "value" v))))
    (merge @m {:network-id (:network-id objs)})))

;; Call this when importing flat json
(defn from-file [filename]
  (from-map (tpn-json/from-file filename)))



;(import-tpn "./test/data/create-parallel-ex.json")