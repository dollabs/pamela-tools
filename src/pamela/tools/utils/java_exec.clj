;
; Copyright © 2019 Dynamic Object Language Labs Inc.
;
; This software is licensed under the terms of the
; Apache License, Version 2.0 which can be found in
; the file LICENSE at the root of this distribution.
;

; Functions to manage process' from clojure. Needed by exec-plant.

(ns pamela.tools.utils.java-exec
  (:import (org.zeroturnaround.exec ProcessExecutor)
           (org.zeroturnaround.exec.listener ProcessListener)
           (org.zeroturnaround.process Processes ProcessUtil)
           (java.util.concurrent TimeUnit)
           (java.io FileOutputStream File)))

(def debug false)

 ;Examples
 ;(start-process "xyz" ["java" "-version"] nil :temp1) ; Output goes nowhere
 ;(start-process "xyz" ["java" "-version"]) ; Output goes to parent process' stdout
 ;(start-process "xyz" ["java" "-version"] nil :temp) ; Output goes to /tmp/xyz.txt or if /tmp does exists, otherwise java.io.tmpdir
 ;
 ;(start-process "xyz" ["java" "-version"] (fn [id exitValue]
 ;                                          (println "finished " id "exit-value" exitValue)))

(defn- make-proc-exit-listener
  "Helper function to setup process-exit-listener"
  [id finish-handler]
  (proxy [ProcessListener] []
    (afterStop [process]
      ; Called when listener has detected that the process has ended with state finished or cancelled
      #_(println "proc finished or cancelled: " id (.exitValue process))
      (finish-handler id (.exitValue process)))))

(defn- exists [file-as-str]
  (.exists (new File file-as-str)))

(defn make-tmp-file
  "Tries to use /tmp before defaulting to java-temp-whatever"
  [prefix suffix]
  (if (exists "/tmp")
    (new File (str "/tmp/" prefix suffix))
    ; createTempFile goes to random folder /var/xxx on macos
    (File/createTempFile prefix suffix)))

(defn- make-temp-file-outstream [prefix suffix]
  (let [tfile (make-tmp-file prefix suffix)
        os (new FileOutputStream tfile)]
    os))

(defn- redirect-output
  "Helper for sensibly redirecting output"
  [id process-executor out-stream]
  (cond (nil? out-stream)
        (.redirectOutput process-executor (System/out))

        (= :temp out-stream)
        (.redirectOutput process-executor (make-temp-file-outstream id "-stdout.txt"))

        :else
        (println "stdout for id" id "will go nowhere")))


(defn start-process
  "id: Some identifier. Used when finish-handler is not-nil to notify which process has ended
   args: Process args including process-name. Ex: [\"java\" \"-version\" ]
   finish-handler: A fun that takes 2 arguments, id and process-exit-value

   Note: stdout of child process is redirected to provided 'out-stream'
         stderr of child process is redirected to provided 'out-stream'
         if 'out-stream' is nil, it will be redirected to parent process' stdout
         if 'out-stream' is :temp, it will be redirected to temp-dir/ prefixed with given id. Ex: /tmp/id.txt
         if 'out-stream' is 'anything-else', it will go nowhere
         "
  [id args & [finish-handler out-stream]]
  (let [as-str (map (fn [x]
                      (str x)) args)
        j-array (into-array as-str)
        _ (when debug (println "Executing command:" id as-str))
        pe (new ProcessExecutor j-array)
        _ (redirect-output (if (keyword id)                 ; output filename should be a string
                             (name id) id) pe out-stream)
        _ (when finish-handler
            (.addListener pe (make-proc-exit-listener id finish-handler)))
        sp (.start pe)]
    {:process-executor pe :started-process sp}))

(defn make-java-process
  "Helper function"
  [started-process]
  (Processes/newJavaProcess (.getProcess started-process)))


(defn wait-for-process
  "Wait indefinitely for the process to finish"
  ([started-process]
   (.waitFor (make-java-process started-process)))

  ([started-process time-out time-out-unit]
   (.waitFor (make-java-process started-process) time-out time-out-unit)))

(defn print-process-info
  "Prints exit value or is alive"
  [started-process]
  (let [jprocess (make-java-process started-process)
        process (.getProcess jprocess)
        alive (.isAlive jprocess)]
    (println (.getDescription jprocess) (if alive "is alive" "") (if-not alive
                                                                   (str "is not alive. Exit value: " (.exitValue process)) ""))))

(defn is-alive [started-process]
  (-> started-process (make-java-process) (.getProcess) (.isAlive)))

(defn cancel-process [started-process]
  "Cancel the process gracefully or forcefully if the graceful termination did not finish for 1 minute"
  (ProcessUtil/destroyGracefullyOrForcefullyAndWait (make-java-process started-process) 1 TimeUnit/MINUTES 1 TimeUnit/MINUTES))
