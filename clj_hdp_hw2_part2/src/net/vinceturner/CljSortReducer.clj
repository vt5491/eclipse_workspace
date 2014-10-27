(ns net.vinceturner.CljSortReducer
  (:gen-class
    :name net.vinceturner.CljSortReducer
    :extends org.apache.hadoop.mapreduce.Reducer
    :methods [
      [reduce [org.apache.hadoop.io.Text java.lang.Object java.lang.Object] void]
    ]
(:import [org.apache.hadoop.mapreduce.Reducer]
           [org.apache.hadoop.io.IntWritable]
           [org.apache.hadoop.io.Text]
           [java.util.StringIterator]
           [java.util.StringTokenizer]
           [org.apache.hadoop.mapreduce.lib.map.WrappedMapper]
           )
    ))

;; (defn -reduce [this key values context]
;;   (def sum 0)
;;   (let [itr (.iterator values) ]
;;     (while
;;         (.hasNext itr)
;;       (do (def sum  (+ sum (.get (.next itr))))))
;;     (.write context key (org.apache.hadoop.io.IntWritable. sum))
;;     )
;;   )

(defn -reduce [this key values context]
  ;; (def actor-list "")
  (def sum 0)
  (let [itr (.iterator values) ]
    (while
        (.hasNext itr)
      (do (def sum  (+ sum (.get (.next itr)))))
      ;; (do (def actor-list (str actor-list (.toString (.next itr)) ";")))
      )
    ;; (.write context key (org.apache.hadoop.io.IntWritable. sum))
    (.write context key (org.apache.hadoop.io.IntWritable. sum))
  )
  )

