(ns tabledemo.core
  (:require [spork.util
             [table :as tbl]
             [io :as io]
             [parsing :as parsing]]
            [tech.ml.dataset :as ds]
              ))

(def the-big-file "big-text.txt")

;;function to make a ~300mb text file.
;;invoke this if you want to follow the demo (takes a bit).
(defn make-file! []
  (-> (for [n (range 10000000)]
        {:x     (rand-int 1000)
         :y     (* (rand) 10000)
         :first (rand-nth ["foo" "bar" "baz"])
         :last  (rand-nth ["foo" "bar" "baz"])})
      (tbl/records->file  the-big-file)))


(comment

;;Read into a somewhat compressed (still persistent, so
;;largeish) spork.util.table form.  Able to hold in memory
;;pretty easily though.

  ;;spork.util.table.table type are column stores based on
  ;;persistent vectors (or rrb-vectors for typed columns).
  ;;They support a full query API as well as typical clojure idioms.

  ;;To aid in parsing speed and memory, you can define
  ;;schemas (outlined in spork.util.parsing/*parsers* by default,
  ;;which is a simple map of {field parsing-function}
  ;;where parsing-function:: string -> a, where a is any type.

  ;;:text or :string fields (synonymous) will use string
  ;;canonicalization via a spork.util.string/->string-pool
  ;;to share string references across the field.  This cuts
  ;;down signifcantly on memory usage since string creation
  ;;will default to creating new references.

(def res (tbl/tabdelimited->table the-big-file
                                  :schema {:x :int
                                           :y :double
                                           :first :text
                                           :last  :text}))
(first (tbl/table-records res))
;;{:x 617, :y 6406.332356017362, :first "baz", :last "baz"}

;;tables are reducible
(into [] (take 1) res)
;;[{:x 617, :y 6406.332356017362, :first "baz", :last "baz"}]

;;use transducer to operate on large set of records directly without
;;holding in memory.  Note: this is eager.

(->> (tbl/tabdelimited->records the-big-file :schema {:x :int
                                                      :y :double
                                                      :first :text
                                                      :last  :text})
     (into [] (filter (fn [r] (< (:x r) 50)))))

)



;;tech.ml.dataset is a recent arrival and provides a really nice
;;means for munging large datasets, ETL, and support datascience/ml
;;workloads.  It's implemented as an abstraction on top of the
;;excellent TableSaw java library, which uses efficient primitive
;;collections to implement a similar, mutable column store.

;;tech.ml.dataset wraps the tablesaw columns with protocols
;;and provides a persistent API via COW semantics, which
;;despite the possible prevalence of copying, given the
;;efficient representation, is quite practical and performant
;;(memory and cpu) in practice.

;;create a tech.ml.dataset (which uses tablesaw underneath)
;;full walkthrough at https://github.com/techascent/tech.ml.dataset/blob/master/docs/walkthrough.md

(comment 
;;This uses compressed COW mutable tables with some fancy primitive
;;collections under the hood, so should be pretty memory efficient
;;(like 10x more due to very limited object references and packed primitives)
(def big-ds  (ds/->dataset the-big-file))

;;tech.ml.dataset provides a similar "seq of maps" interface
;;along with a plethora of selection, query, filter, grouping, etc.
;;API functions.
(first (ds/mapseq-reader big-ds))
;;{"x" 617, "y" 6406.3325, "first" "baz", "last" "baz"}
)


