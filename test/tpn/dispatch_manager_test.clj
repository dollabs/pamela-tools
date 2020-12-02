(ns tpn.dispatch-manager-test
  (:require [tpn.test-support :as tpn-support]
            [pamela.tools.dispatcher_manager.core :as dispatcher]
            [pamela.tools.utils.rabbitmq :as rmq]
            [me.raynes.fs :as fs]
            [clojure.test :refer :all]
            [clojure.pprint :refer :all]
            [clojure.string :as str]
            [pamela.tools.utils.tpn-json :as tpn_json]
            [pamela.tools.mct-planner.planner :as planner]))

(defonce dapp nil)

(defn init-dapp []
  (when-not dapp
    (dispatcher/init dispatcher/default-plant-id rmq/default-exchange rmq/default-host rmq/default-port)
    (def dapp true)))

(defn replace-fnames-with-dot
  "fnames is list of stringified file paths
  separator-dir is a directory name in the path.
  Return a string such that / is replace with .
  the returned string is a suitable file name (flattened) usable for ready reference.
  "
  [fnames separator-dir]
  (map (fn [fname]
         (let [tokens (fs/split fname)                      ; convert to tokens
               [_ after] (split-with (fn [x] (not (= x separator-dir)))
                                     tokens)
               after  (str/join "." after)]
           [fname after]))
       fnames))

; For use in repl only for now
(defn dispatch-all-tpn []
  (init-dapp)
  (dispatcher/set-force-plant-id true)
  (let [fnames     (tpn-support/get-tpn-files-by-ext)
        remove-ext (fn [fname]
                     (first (str/split fname #".tpn.json")))
        disp-ids   (replace-fnames-with-dot fnames "test")]
    (println "Total TPNs" (count fnames))
    (doseq [[tpn-file mission-id] disp-ids #_(take 1 disp-ids)]
      (println "dispatching" (remove-ext mission-id) mission-id tpn-file)
      (let [tpn (tpn_json/from-file tpn-file)
            {bindings :bindings} (planner/solve-with-node-bindings tpn nil 1)]
        ;(pprint bindings)
        (dispatcher/send-start-msg (remove-ext mission-id) mission-id tpn bindings true)))))