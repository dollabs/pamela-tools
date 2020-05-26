;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(defproject
  dollabs/pamela-tools
  "0.2.0-SNAPSHOT"
  :repositories
  [["clojars" {:url "https://repo.clojars.org/"}]
   ["maven-central" {:url "https://repo1.maven.org/maven2"}]
   ; jahmm repo is needed for HMM related code
   ["jahmm repo" {:url "http://repo.springsource.org/libs-release-remote"}]
   ]
  :dependencies
  [; boot-lein-generate is not needed but always added by when this file is created
   ;[onetom/boot-lein-generate "0.1.3"]
   [org.clojure/clojure "1.8.0"]
   [adzerk/boot-test "1.2.0" :scope "test"]
   [com.novemberain/langohr "3.3.0"]
   [net.mikera/core.matrix "0.50.0"]
   [org.apache.commons/commons-math3 "3.6.1"]
   [incanter "1.5.5"]
   [me.raynes/fs "1.4.6"]
   [net.mikera/imagez "0.12.0"]
   [be.ac.ulg.montefiore.run.jahmm/jahmm "0.6.2"]
   [com.novemberain/monger "3.0.2"]
   [org.zeroturnaround/zt-exec "1.10"]
   [org.zeroturnaround/zt-process-killer "1.4"]
   [com.hierynomus/sshj "0.17.2"]
   [ruiyun/tools.timer "1.0.1"]
   [automat "0.2.0"]
   [org.clojure/data.csv "0.1.3"]
   [org.clojure/core.async "1.2.603"]
   [org.clojure/data.json "0.2.6"]
   [org.clojure/tools.cli "0.3.5"]]
  :source-paths
  ["test"]
  :resource-paths
  ["src"])