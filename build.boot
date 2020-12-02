;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

(def project 'dollabs/pamela-tools)
(def version "0.2.0-SNAPSHOT")
(def description "Pamela Tools")
(def project-url "https://github.com/dollabs/pamela-tools")

(set-env!
  :source-paths #{"test"}
  :resource-paths #{"src"}
  :dependencies '[[org.clojure/clojure "1.8.0"]
                  ;[onetom/boot-lein-generate "RELEASE" :scope "test"]
                  [adzerk/boot-test "RELEASE" :scope "test"]

                  [com.novemberain/langohr "3.3.0"]
                  [net.mikera/core.matrix "0.50.0"]
                  [org.apache.commons/commons-math3 "3.6.1"]
                  [incanter "1.5.5"]
                  [me.raynes/fs "1.4.6"]
                  [net.mikera/imagez "0.12.0"]
                  ;Loading src/pamela_tools/utils/mongo/db.clj...
                  ;CompilerException java.lang.RuntimeException: Unable to find static field: ACKNOWLEDGED in class com.mongodb.WriteConcern, compiling:(monger/core.clj:59:1)
                  ; Monger 3.1.0 does not load in my repl but compiles fine!
                  [com.novemberain/monger "3.0.2"]
                  [org.zeroturnaround/zt-exec "1.10"]
                  [org.zeroturnaround/zt-process-killer "1.4"]
                  [com.hierynomus/sshj "0.17.2"]
                  [ruiyun/tools.timer "1.0.1"]
                  [automat "0.2.0"]

                  [org.clojure/data.csv "0.1.3"]
                  [org.clojure/core.async "1.2.603"]
                  [org.clojure/data.json "0.2.6"]
                  [org.clojure/tools.cli "0.3.5"]])

(require
  '[adzerk.boot-test :refer [test]])

(task-options!
  pom {:project     project
       :version     version
       :description description
       :url         project-url
       :scm         {:url project-url}
       :license     {"Apache-2.0" "http://opensource.org/licenses/Apache-2.0"}})

(deftask check-errors
         "Use aot to check for errors"
         []
         (println "Compiling files")
         (comp (aot :all true) (target :dir #{"target"} :no-clean true)))

(deftask build
         "Collects clj files "
         []
         (println "task: build")
         (comp (pom) (jar) (install) (target :dir #{"target"})))

(deftask uber-dispatcher
         "Create uber jar for dispatcher"
         []
         (println "Creating uber jar for dispatcher")
         (comp (aot :namespace #{'pamela.tools.dispatcher.dispatch-app}) (uber) (jar :file (str "dispatcher-" version ".jar")
                                                                                     :main 'pamela.tools.dispatcher.dispatch-app) (target :no-clean true)))

(deftask uber-dispatcher-manager
         "Create uber jar for dispatcher-manager"
         []
         (println "Creating uber jar for dispatcher-manager")
         (comp (aot :namespace #{'pamela.tools.dispatcher_manager.core}) (uber) (jar :file (str "dispatcher-manager-" version ".jar")
                                                                                     :main 'pamela.tools.dispatcher_manager.core) (target :no-clean true)))

(deftask uber-plant-sim
         "Create uber jar for uber-plant-sim"
         []
         (println "Creating uber jar for uber-plant-sim")
         (comp (aot :namespace #{'pamela.tools.plant.plant-sim}) (uber) (jar :file (str "plant-sim-" version ".jar")
                                                                             :main 'pamela.tools.plant.plant-sim) (target :no-clean true)))

(deftask uber-rmq-logger
         "Create uber jar for rmq-logger"
         []
         (println "Creating uber jar for rmq-logger")
         (comp (aot :namespace #{'pamela.tools.rmq-logger.core}) (uber) (jar :file (str "rmq-logger-" version ".jar")
                                                                             :main 'pamela.tools.rmq-logger.core) (target :no-clean true)))

(deftask uber-log-player
         "Create uber jar for rmq-log-player"
         []
         (println "Creating uber jar for rmq-log-player")
         (comp (aot :namespace #{'pamela.tools.rmq-logger.log-player}) (uber) (jar :file (str "rmq-log-player-" version ".jar")
                                                                                   :main 'pamela.tools.rmq-logger.log-player) (target :no-clean true)))

(deftask uber-clock
         "Create uber jar for sim clock"
         []
         (println "Creating uber jar for simulated clock")
         (comp (aot :namespace #{'pamela.tools.utils.clock-app}) (uber) (jar :file (str "sim-clock-" version ".jar")
                                                                                   :main 'pamela.tools.utils.clock-app) (target :no-clean true)))

; boot uber-dispatcher uber-dispatcher-manager uber-plant-sim uber-rmq-logger uber-log-player uber-clock

#_(deftask uber-exp-charts
         "Create uber jar for plotting monte carlo experiment stats"
         []
         (println "Create uber jar for plotting monte carlo experiment stats")
         (comp (aot :namespace #{'pamela.tools.mct-planner.plot-exp-charts}) (uber) (jar :file (str "exp-charts-" version ".jar")
                                                                                   :main 'pamela.tools.mct-planner.plot-exp-charts) (target :no-clean true)))

#_(deftask uber-experiments
         "Create uber jar for running monte carlo experiments"
         []
         (println "Create uber jar for running monte carlo experiments")
         (comp (aot :namespace #{'pamela.tools.mct-planner.experiments}) (uber) (jar :file (str "experiments-" version ".jar")
                                                                                         :main 'pamela.tools.mct-planner.experiments) (target :no-clean true)))
; WIP
#_(deftask build-all
           "Build all uber jars"
           []

           (comp (build)
                 (uber-tailer)
                 (uber-event-player)
                 (target)))

; To generate project.clj and keep in sync with boot file.
;   boot -d onetom/boot-lein-generate generate
