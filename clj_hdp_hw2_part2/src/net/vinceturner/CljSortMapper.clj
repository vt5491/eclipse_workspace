(ns net.vinceturner.CljSortMapper
  (:gen-class
    :name net.vinceturner.CljSortMapper
    :extends org.apache.hadoop.mapreduce.Mapper
    :methods [
       [map [java.lang.Object org.apache.hadoop.io.Text org.apache.hadoop.mapreduce.lib.map.WrappedMapper] void]
        [getfield [String] Object]
        [setfield [String Object] void]
       ]
    :state state
    :init init
  (:import [org.apache.hadoop.mapreduce.Mapper]
           [org.apache.hadoop.io.IntWritable]
           [org.apache.hadoop.io.Text]
           [java.util.StringIterator]
           [java.util.StringTokenizer]
           [org.apache.hadoop.mapreduce.lib.map.WrappedMapper]
           )
  )
)

(defn -init []
  "store our fields as a hash"
  [[] (atom {
             :one (new org.apache.hadoop.io.IntWritable 1),
             ;; :word (new org.apache.hadoop.io.Text)
             :word (new org.apache.hadoop.io.IntWritable)
             })])

(defn getfield
  [this key]
  (@(.state this) key))

;; little functions to safely set the fields.
(defn setfield
  [this key value]
      (swap! (.state this) into {key value}))

(defn StringToNumber [str]
  (let [n (read-string str)]
       (if (number? n) n nil)))

(defn -map [this key value context]
  ;; (println "CljSortMapper->map: entered the function baby")
  ;; (prn "CljSortMapper->map: value=" value)
  (def itr (new java.util.StringTokenizer (str value)))
  
  (let [word (getfield this :word),
        one (getfield this :one),
        parse-vec (clojure.string/split (str value) #"\t")]
    ;; (doseq [token (clojure.string/split (str value) #"[ ]+")] (.set word token) (prn "do-seq: word=" word) (.write context word one))
    ;; (doseq [token (clojure.string/split (str value) #"\t")]
    ;;   (.set word token)
    ;;   (prn "do-seq: word=" word)
    ;;   (.write context word one))
    ;; (prn "map: parse-vec=" parse-vec)
    
    ;; key is the actor count
    ;; (prn "type of parse-vec 1= " (type (nth parse-vec 1)))
    ;; (.set word (org.apache.hadoop.io.IntWritable. (StringToNumber (nth parse-vec 1))))
    (.set word (StringToNumber (nth parse-vec 1)))
    ;; (.set word (org.apache.hadoop.io.Text. (nth parse-vec 1)))
    ;; (prn "map: word=" (.toString word))
    ;; (.set word (org.apache.hadoop.io.Text. (nth parse-vec 0)))
    ;; (prn "map: word (movie)=" (.toString word) ",value (actor)=" (nth parse-vec 0))
    ;; value is the actor
    ;; value is one
    ;; (.write context word (org.apache.hadoop.io.Text. (nth parse-vec 0)))
    ;; (.write context word (getfield this :one))
    ;; value is the actor
    ;; (prn "map: actor=" (nth parse-vec 0))
    (.write context word (org.apache.hadoop.io.Text. (nth parse-vec 0)))
    ;; (.write context word 7)
    )
   ;; (println "CljSortMapper->-map: done with let, done with sub")
  )


  
;; we expose them as external methods
;; (defn -getfield
;;   [this str]
;;   (let [key (keyword str)]
;;     (@(.state this) key)))

;; ;; little functions to safely set the fields.
;; (defn -setfield
;;   [this str value]
;;   (let [key (keyword str)]
;;        (swap! (.state this) into {key value})))

;; (defn  getLocation
;;   [this]
;;   (getfield this :location))

;; (defn  -getLocation
;;   [this]
;;   (getfield this :location))

;; (defn  getOne
;;   [this]
;;   (getfield this :one))

;; (defn  -getOne
;;   [this]
;;   (println "now in -getOne")
;;   (getfield this :one))







