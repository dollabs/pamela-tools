;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.utils.mongo.state
  (:import (clojure.lang Keyword)))

; To keep mongodb related information
(def debug? false)

; Store connection and other information
(def state (atom {}))

(defn update-state!
  "Updates state atom with given k v"
  [k v]
  "Rewrite key with new value"
  (swap! state conj {k v}))

(defn remove-from-state
  "Removed given key from state atom"
  [k]
  (swap! state dissoc k))

(defn get-connection
  "Returns connection value of :connection"
  []
  (:connection @state))

(defn get-state
  "Returns value of given key(coverts to keyword if string)"
  [key]
  (cond (instance? Keyword key)
        (key @state)

        (instance? String key)
        ((keyword key) @state)

        :else
        (when debug?
          (binding [*out* *err*]
            (println "unknow type of key " (type key) key)))))



