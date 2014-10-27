(defproject clj_hdp_hw2_part2 "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 [org.clojure/clojure "1.6.0"]
                 [clj-time "0.9.0-beta1"]
                 [org.apache.hadoop/hadoop-common "2.4.0"]
                 [org.apache.hadoop/hadoop-mapreduce-client-core "2.4.0"]
                 [org.apache.hadoop/hadoop-mapreduce-client-jobclient "2.4.0"]
                 [org.apache.hadoop/hadoop-mapreduce-client-common "2.4.0"]
                 [org.apache.hadoop/hadoop-mapreduce-client-shuffle "2.4.0"]
                 [org.apache.hadoop/hadoop-core "1.2.1"]
    ]
  :plugins [[cider/cider-nrepl "0.8.0-SNAPSHOT"]]
  :aot :all
  :main net.vinceturner.Hw2Part2
)
