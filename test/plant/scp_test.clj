;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(ns plant.scp-test
  (:require #_[plant.scp :refer :all]
            [clojure.test :refer :all])
  )

; This test is conflicting with incanter dependencies.
; Since plant interface is being tested elsewhere, this upload test seens unnecessary.
#_(deftest upload-test
  (testing "Upload from jar"
    (println "Upload test from jar to lispmachine")
    (is (not= nil (test-send-file)))))