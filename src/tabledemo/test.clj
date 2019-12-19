(ns tabledemo.test
  (:require  [spork.util
              [table :as tbl]
              [string :as pool]]
             [clojure.java [io :as io]]
             [tech.ml.dataset :as ds])
  (:import [java.text SimpleDateFormat]))

(def date-format "yyyy-mm-dd")
(def sdf (SimpleDateFormat. date-format))

(set! *warn-on-reflection* true)
(defn random-date []
  (.format ^SimpleDateFormat sdf
           (java.util.Date. (rand-int 80) (rand-int 12) (rand-int 30))))

;; I am trying to parse a 50MB CSV file. ~2500 rows, ~5500 columns, one column
;; is strings (date as yyyy-mm-dd) and the rest is floats with lots of empty
;; points. I need to be able to access all the data so would like to realize the
;; full file, which should be possible at that size.

(defn test-rows []
  (for [i (range 2500)]
    (into [(random-date)]
          (repeatedly 5499
               (fn [] (when (< (rand) 0.3)
                        (* (rand-int 1000) 1.0)))))))

(def the-test-file "the-test-file.csv")
(def fields (into ["SomeDate"]
                  (for [i (range 5499)]
                    (str "X_" i))))

;;create a random test file if don't already have one..
(defn spit-test-file! []
  (with-open [out (io/writer the-test-file)]
    (.write out (str (clojure.string/join "," fields)
                     \newline))
    (doseq [r (test-rows)]
      (.write out (str (clojure.string/join "," r) \newline)))))


(defn maybe-double? [^String x]
  (when (not= x "")
    (Double/parseDouble x)))

;;easy way. using spork api.
(comment
  (def res
    (tbl/tabdelimited->table the-test-file
                             :delimiter ","
                             :schema (into {:SomeDate :text}
                                           (for [f fields]
                                             [f maybe-double]))))
  )


;;Tablesaw can't handle this  since it tries to parse the dates literally.
;;It barfs on the input, dunno why.
#_(def res  (ds/->dataset the-test-file))

;;Manual parsing (by row):

(defn parse-row [idx->f xs]
  (map-indexed (fn [idx v] ((idx->f idx) v))))

;;hard way, but more control...
(defn custom-parse [l]
  (let [xs  (clojure.string/split l #",")]
    (into [(first xs)] (map maybe-double?) (subvec xs 1))))

(defn file->records []
  (with-open [rdr (io/reader the-test-file)]
    (mapv custom-parse (rest (line-seq rdr)))))
