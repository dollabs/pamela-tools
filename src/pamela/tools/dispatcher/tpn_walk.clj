;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.dispatcher.tpn-walk
  (:require [pamela.tools.dispatcher.tpnrecords :as tpnr])
  )

(defmulti walk-tpn
          (fn [obj _ _ _]
            (type obj)))

(defn set-visited! [obj visited]
  (var-set visited (conj @visited (:uid obj)))
  #_(println "set-visited!" @visited "\n"))

(defn visited? [obj visited]
  (contains? @visited (:uid obj)))

(defn print-walk [obj _]
  (println "Visited" (:uid obj) (type obj)))

(defn begin-walk [obj func objects]
  (if-not obj
    (println "begin-walk obj is nil"))
  (if-not objects
    (println "begin-walk objects is nil"))
  (if-not func
    (println "walk fun is nil. Assuming print-walk"))

  (with-local-vars [visited #{}]
    (walk-tpn obj (if func func print-walk) objects visited)))

(defmethod walk-tpn :default [obj func _ _]
  (binding [*out* *err*]
    (println "walk-tpn handle \ntype:" (type obj) "\nfunc:" func "\nobj" obj)))

(defn walk-tpn-begin [obj func objects visited]
  (if-not (visited? obj visited)
    (do (func obj objects)
        (set-visited! obj visited)
        (doseq [act (:activities obj)]
          #_(println "walking" act " of " (:activities obj))
          (walk-tpn (act objects) func objects visited)))
    #_(println "already visited" (:uid obj) (:tpn-type obj)))
  )
; When dispatching on classnames, we need file path not namespace; Notice pamela_tools  instead of pamela-tools
(defmethod walk-tpn pamela.tools.dispatcher.tpnrecords.p-begin [obj func objects visited]
  #_(println "\nwalk p-begin" (:uid obj) (:end-node obj))
  (walk-tpn-begin obj func objects visited))

(defmethod walk-tpn pamela.tools.dispatcher.tpnrecords.c-begin [obj func objects visited]
  #_(println "\nwalk c-begin" (:uid obj) (:end-node obj))
  (walk-tpn-begin obj func objects visited))

(defn walk-tpn-state [obj func objects visited]
  (if-not (visited? obj visited)
    (do (func obj objects)
        (set-visited! obj visited)
        (when (first (:activities obj))
          (walk-tpn ((first (:activities obj)) objects) func objects visited)))
    #_(println "already visited" (:uid obj) (:tpn-type obj))))

(defmethod walk-tpn pamela.tools.dispatcher.tpnrecords.state [obj func objects visited]
  (walk-tpn-state obj func objects visited))

(defmethod walk-tpn pamela.tools.dispatcher.tpnrecords.p-end [obj func objects visited]
  (if-not (visited? obj visited)
    (walk-tpn-state obj func objects visited)
    #_(println "already visited" (:uid obj) (:tpn-type obj))))

(defmethod walk-tpn pamela.tools.dispatcher.tpnrecords.c-end [obj func objects visited]
  (if-not (visited? obj visited)
    (walk-tpn-state obj func objects visited)
    #_(println "already visited" (:uid obj) (:tpn-type obj))))


(defn walk-tpn-activity [obj func objects visited]
  (if-not (visited? obj visited)
    (do (func obj objects)
        (set-visited! obj visited)
        (walk-tpn ((:end-node obj) objects) func objects visited))
    #_(println "already visited" (:uid obj) (:tpn-type obj))))

(defmethod walk-tpn pamela.tools.dispatcher.tpnrecords.null-activity [obj func objects visited]
  (walk-tpn-activity obj func objects visited))

(defmethod walk-tpn pamela.tools.dispatcher.tpnrecords.activity [obj func objects visited]
  (walk-tpn-activity obj func objects visited))

(defmethod walk-tpn pamela.tools.dispatcher.tpnrecords.delay-activity [obj func objects visited]
  (walk-tpn-activity obj func objects visited))

; Collect tpn objects
; --------------------------------------------------------------------------

(defn collect-tpn [netid objs]
  "Walk the tpn and returns a map of nodes and edges including constraints but not network-id"
  #_(println "objs")
  #_(clojure.pprint/pprint objs)
  (with-local-vars [m {}]
    (let [network (netid objs)
          bnode (:begin-node network)
          ]
      (var-set m (assoc @m netid network))
      #_(println "collect-tpn" objs)
      #_(clojure.pprint/pprint @m)
      (begin-walk (bnode objs) (fn [obj _]
                                 ;(println "got " (:uid obj) (type obj))
                                 (var-set m (assoc @m (:uid obj) obj #_(merge obj {:checkme (.getSimpleName (type obj))})))
                                 #_(clojure.pprint/pprint @m)
                                 (when-not (empty? (:constraints obj))
                                   #_(println "constraints" (:constraints obj))
                                   (doseq [c (:constraints obj)]
                                     (var-set m (assoc @m c (c objs)))))
                                 ) objs)
      @m
      )))

(defn collect-tpn-with-netid [netid objs]
  "Walk the tpn and returns a map containing nodes, edges, constraints and network-id"
  (merge (collect-tpn netid objs) {:network-id netid}))

(defn collect-tpn-ids [netid objs]
  "Walks the TPN and returns a vector containing ids of nodes and edges along with netid"
  (with-local-vars [ids [netid]]
    (begin-walk (tpnr/get-begin-node netid objs) (fn [obj _]
                                                  (var-set ids (conj @ids (:uid obj)))
                                                  ) objs)
    @ids))