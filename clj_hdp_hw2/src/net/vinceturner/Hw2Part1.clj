(ns net.vinceturner.Hw2Part1
  (:gen-class
    :main true
    :name net.vinceturner.Hw2Part1
    )
  (:import [org.apache.hadoop.mapreduce.Mapper]
           [org.apache.hadoop.mapreduce.Mapper]
           [org.apache.hadoop.mapreduce.Job]
           [org.apache.hadoop.conf.Configuration]
           [org.apache.hadoop.mapreduce.lib.input.FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output.FileOutputFormat]
           [org.apache.commons.logging.Log]
           [org.apache.commons.logging.LogFactory]
           ;; [org.apache.hadoop.util.GenericOptionsParser]
           )
  (:require [clj-time.local :as l])
)

(defn -main
  "The application's main function"
  [& args]
  ;; Configuration conf = new Configuration();
  (def conf (org.apache.hadoop.conf.Configuration.))
  ;; String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  ;; (def otherArgs (GenericOptionsParser. conf args))
  ;; (prn "otherArgs=" otherArgs)
  ;; (def input-file (or ()))
  (prn "args[0]2=" (nth args 0))
  (prn "length args=" (count args))
  ;; (and (>= (count a) 1) (nth a 1))
  (if (>= (count args) 1)
    (def input-dir (new org.apache.hadoop.fs.Path (str (nth args 0))))
    (def input-dir (new org.apache.hadoop.fs.Path (str (System/getProperty "user.dir") "/data/movie-test"))))
  (if (>= (count args) 2)
    (def output-dir (new org.apache.hadoop.fs.Path (str (nth args 1))))
    (def output-dir (new org.apache.hadoop.fs.Path (str (System/getProperty "user.dir") "/hdp-out/output_" (l/format-local-time (l/local-now) :date-hour-minute-second)))))

  (prn " input-dir=" input-dir)
  (prn " output-dir=" output-dir)

  ;; (prn "Hw2Part1.clj: args=" args)
    (def conf (new org.apache.hadoop.conf.Configuration))
  
    (def job (new org.apache.hadoop.mapreduce.Job conf "clj-hw2-part1"))
    (def mapper (new net.vinceturner.CljFilmActorMapper))
    (.setMapperClass job (class mapper))
  ;; (prn "job MapperClass=" (.getMapperClass job))
    (def reducer (new net.vinceturner.CljFilmActorReducer))
    (.setReducerClass job (class reducer))
  ;; (prn "job ReducerClass=" (.getReducerClass job))
  ;; (.setJarByClass job (class net.vinceturner.CljWordCount))
    (.setJarByClass job  net.vinceturner.Hw2Part1)
    (.setOutputKeyClass job  org.apache.hadoop.io.Text)
    ;;vt-x (.setOutputValueClass job  org.apache.hadoop.io.IntWritable)
    (.setOutputValueClass job  org.apache.hadoop.io.Text)
    ;; (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath job  (new org.apache.hadoop.fs.Path "/home/vturner/workspace/clj_hdp_hw2/data/movie-test"))
    (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath job  input-dir)
    ;; (org.apache.hadoop.mapreduce.lib.output.FileOutputFormat/setOutputPath job  (new org.apache.hadoop.fs.Path (str "/home/vturner/vtstuff/hadoop_stuff/output_" (l/format-local-time (l/local-now) :date-hour-minute-second))))
(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat/setOutputPath job  output-dir)    
  ;; ;; call it
    (prn "now calling waitForCompletion")
    (.waitForCompletion job true)
    (prn "back from waitForCompletion")
  )
