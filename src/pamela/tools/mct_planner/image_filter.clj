;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns pamela.tools.mct-planner.image-filter
  (:require [me.raynes.fs :as fs]
            [clojure.string :as str]
            [clojure.pprint :refer :all]
            [mikera.image.core :as img-core])
  (:import (java.awt Frame)))

; Helper class to filter images on certain learning attributes and show them.
; Given some criteria as :lambda-f 0, find all directories that match the criteria and show each image/chart in a window


; dir name n-regions-5-bsd-0.75-lpdf-0.005-lambda-s-0-lambda-f-0
;Learner config example
{:b-sd-s 0.75,
 :b-sd-f 0.75,
 :lambda-sf-ratio 1000,
 :lambda-s 0,
 :lowest-pdf 0.005,
 :lambda-f 0}

(def data-path "./long-learner-data")                       ;top level directory

(defn get-dirs
  "Gives list of directories in 'data-path' that contain charts showing learned pdf" []
  (let [dirs (fs/iterate-dir data-path)
        top-dir (second (first dirs))]
    (filter (fn [name]
              (clojure.string/starts-with? name "n-regions"))
            top-dir)))

; dirs is a sequence of [root dirs files]
; we are interested in root (java File object)
(defonce dirs (get-dirs))

(defn init-dirs []
  (def dirs (get-dirs)))

(defn test-filter-fn [f-name]
  (and (str/includes? f-name "lambda-f-0")
       (str/includes? f-name "lambda-s-0.001")
        (str/includes? f-name "regions-2")
       (str/includes? f-name "bsd-2")
        ))

(defn make-filter-when-lambda-s-0 [n-regions]
  (fn [f-name]
    (and (str/includes? f-name "lambda-s-0-")
         (str/includes? f-name (str "n-regions-" n-regions)))
    ))

(defn make-filter-when-lambda-s-gt-0 [n-regions]
  (fn [f-name]
    (and (or (str/includes? f-name "lambda-s-0.001-")
             #_(str/includes? f-name "lambda-s-0.01-"))
         (not (str/includes? f-name "lpdf-0.01-"))
         #_(str/includes? f-name "lpdf-0.01-")
         (str/includes? f-name (str "n-regions-" n-regions)))
    ))

(defn select-files [filter-fn]
  (sort (filter filter-fn dirs)) )

; Jframe windows are resource hungry. Cache them so that I can automate cleaning them up.
(defonce charts (atom []))

(defn make-file-path [d-name f-name]
  ; assume parent dir
  (str data-path "/" d-name "/" f-name))

(defn read-charts [sel-dirs]
  (let [f-name "iteration-100000.png"]
    (reduce (fn [res d-name]
              (println "load-file" (make-file-path d-name f-name))
              (let [chart (img-core/load-image (make-file-path d-name f-name))
                    jframe (img-core/show chart :title d-name)]
                (conj res {:d-name d-name
                           :f-name f-name
                           :chart chart
                           :jframe jframe}))) [] sel-dirs)))

(defn hide-charts
  "Closes the window and cleans up all resources"
  []
  (doseq [cha @charts]
    (.dispose (:jframe cha))))

(defn release-charts
  "resource cleanup"  []
  (hide-charts)
  (reset! charts []))

(defn cycle-charts
  "Iterates through charts and brings them to front one after the other." []
  (doseq [cha @charts]
    (.toFront (:jframe cha))
    (Thread/sleep 500)))

(defn iconify-charts
  "Minimizes all charts" []
  (doseq [cha @charts]
    (.setState (:jframe cha) (Frame/ICONIFIED))))

(defn show-charts
  "Brings all charts to front" []
  (doseq [cha @charts]
    (.show (:jframe cha))))

(defn make-charts
  "Reads charts from file system and shows each in a window"
  [filt-fn]
  (let [sel (select-files filt-fn)
        ;_ (pprint sel)
        chs (read-charts sel)]
    (release-charts)
    (println )
    (swap! charts into chs)
    (str "Made " (count chs) " Charts")))

(defn print-dirs [blah]
  ;(pprint blah)
  (doseq [d blah]
    (println (.getName (first d)))
    ))

; close all open windows
; cycle through all windows
; sort widows
