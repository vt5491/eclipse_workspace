(ns net.vinceturner.Hw2Part2
  (:gen-class
    :main true
    :name net.vinceturner.Hw2Part2
    )
  (:import [org.apache.hadoop.mapreduce.Mapper]
           [org.apache.hadoop.mapreduce.Reducer]
           [org.apache.hadoop.mapred.lib.IdentityReducer]
           [org.apache.hadoop.mapreduce.Job]
           [org.apache.hadoop.conf.Configuration]
           [org.apache.hadoop.mapreduce.lib.input.FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output.FileOutputFormat]
           [org.apache.commons.logging.Log]
           [org.apache.commons.logging.LogFactory]
           [org.apache.hadoop.io.LongWritable]
           [org.apache.hadoop.io.RawComparator]
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
    (def output-dir (new org.apache.hadoop.fs.Path (str (System/getProperty "user.dir") "/hdp-out/output_" (clojure.string/replace  (l/format-local-time (l/local-now) :date-hour-minute-second) #":" "")))))

  (prn " input-dir=" input-dir)
  (prn " output-dir=" output-dir)

  ;; (prn "Hw2Part1.clj: args=" args)
    (def conf (new org.apache.hadoop.conf.Configuration))
  
    (def job (new org.apache.hadoop.mapreduce.Job conf "clj-hw2-part2"))
    (def mapper (new net.vinceturner.CljActorMapper))
    (.setMapperClass job (class mapper))
  ;; (prn "job MapperClass=" (.getMapperClass job))
    (def reducer (new net.vinceturner.CljActorReducer))
    (.setReducerClass job (class reducer))
  ;; (prn "job ReducerClass=" (.getReducerClass job))
  ;; (.setJarByClass job (class net.vinceturner.CljWordCount))
    (.setJarByClass job  net.vinceturner.Hw2Part2)
    (.setOutputKeyClass job  org.apache.hadoop.io.Text)
    (.setOutputValueClass job  org.apache.hadoop.io.IntWritable)
    ;; (.setOutputValueClass job  org.apache.hadoop.io.Text)
    ;; (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath job  (new org.apache.hadoop.fs.Path "/home/vturner/workspace/clj_hdp_hw2/data/movie-test"))
    (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath job  input-dir)
    ;; (org.apache.hadoop.mapreduce.lib.output.FileOutputFormat/setOutputPath job  (new org.apache.hadoop.fs.Path (str "/home/vturner/vtstuff/hadoop_stuff/output_" (l/format-local-time (l/local-now) :date-hour-minute-second))))
(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat/setOutputPath job  output-dir)    
  ;; ;; call it
    (prn "now calling waitForCompletion")
    (.waitForCompletion job true)
    (prn "back from waitForCompletion")

    ;; mr #2
    (def conf (new org.apache.hadoop.conf.Configuration))
    (def output-dir-2 (new org.apache.hadoop.fs.Path (str output-dir "_mr2")))
    ;; (def output-dir-2 (new org.apache.hadoop.fs.Path (str (System/getProperty "user.dir") "/hdp-out/output_" (l/format-local-time (l/local-now) :date-hour-minute-second) "-mr2")))
    (def job2 (new org.apache.hadoop.mapreduce.Job conf "clj-hw2-part2-mr2"))
    (def mapper (new net.vinceturner.CljSortMapper))
    (.setMapperClass job2 (class mapper))
    ;; set the reducer to the identy reducer ie. do nothing
    ;; (.setReducerClass job (org.apache.hadoop.mapred.lib.IdentityReducer))
    ;; (.setReducerClass job (class (new org.apache.hadoop.mapred.lib.IdentityReducer)))
    (.setReducerClass job2 (class (new org.apache.hadoop.mapreduce.Reducer)))
    (.setJarByClass job2  net.vinceturner.Hw2Part2)
    (.setOutputKeyClass job2  org.apache.hadoop.io.IntWritable)
    ;; (.setOutputKeyClass job2  org.apache.hadoop.io.Text)
    (.setOutputValueClass job2  org.apache.hadoop.io.Text)
    ;; job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
    ;; job.setSortComparatorClass(DescendingIntComparable.class)
    ;; make it sort descendingly
    ;; Note: I think I need to implement a separate comparable class.  I currently do
    ;; not have the time to figure this out, as writing that class in clojure involves a lot
    ;; of new things I have yet to learn.
    ;; (.setSortComparatorClass job2 (class org.apache.hadoop.io.LongWritable$DecreasingComparator))
;;     job.setMapOutputKeyClass(Text.class)
;; job.setMapOutputValueClass(IntWritable.class);
    ;;vt- these are new
    ;; (.setMapOutputKeyClass job2 org.apache.hadoop.io.IntWritable)
    ;; (.setMapOutputValueClass job2 org.apache.hadoop.io.Text)
    (prn "input-dir=" output-dir)
    (prn "output-dir=" output-dir-2)

    (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath job2  output-dir)
    (org.apache.hadoop.mapreduce.lib.output.FileOutputFormat/setOutputPath job2  output-dir-2)    
    ;; ;; call it
    (prn "now calling waitForCompletion mr2")
    (.waitForCompletion job2 true)
    (prn "back from waitForCompletion mr2")
  )
