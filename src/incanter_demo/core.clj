(ns incanter-demo.core
  (:use [incanter.core]
        [incanter.stats]))

(def age [{:user-id 1 :age 20}
          {:user-id 2 :age 25}
          {:user-id 3 :age 25}
          {:user-id 4 :age 30}
          {:user-id 5 :age 30}
          {:user-id 6 :age 30}
          {:user-id 7 :age 35}
          {:user-id 8 :age 35}])

(def gender [{:user-id 1 :gender :m}
             {:user-id 2 :gender :m}
             {:user-id 3 :gender :f}
             {:user-id 4 :gender :m}
             {:user-id 5 :gender :m}
             {:user-id 6 :gender :f}
             {:user-id 7 :gender :f}
             {:user-id 8 :gender :m}])

(def activity [{:user-id 1 :activity 1}
               {:user-id 2 :activity 3}
               {:user-id 3 :activity 8}
               {:user-id 4 :activity 5}
               {:user-id 5 :activity 4}
               {:user-id 6 :activity 6}
               {:user-id 7 :activity 9}
               {:user-id 8 :activity 2}])

(def location [{:user-id 1 :lat 45 :lon -120}
               {:user-id 2 :lat 45 :lon -122}
               {:user-id 3 :lat 44 :lon -121}
               {:user-id 4 :lat 44 :lon -122}
               {:user-id 5 :lat 44 :lon -120}
               {:user-id 6 :lat 44 :lon -121}
               {:user-id 7 :lat 45 :lon -120}
               {:user-id 8 :lat 45 :lon -122}])

(defn concat-maps [m & ms]
  (let [args (if (nil? ms) m
                 (conj ms m))]
    (reduce (fn [res x] (reduce conj res x))
            []
            args)))

(defn group-by-merge [f coll]
        (reduce (fn [x [k v]]
                  (assoc x k (apply merge v)))
                {}
                (group-by f coll)))

(def user-info-indexed (group-by-merge :user-id (concat-maps age gender activity location)))
(def user-info (vals user-info-indexed))

(def ds (to-dataset user-info))

;; some useful functions
(head ds)
(summary ds)


;; ---- Selecting Rows ----

;; ($ rows cols dataset)
;; alias for ...
;; (sel dataset :rows rows :cols cols)
($ (range 2 4) :all ds)
($ [0 1 2] [:age :gender] ds)

(sel ds :rows (range 4) :cols [:age :gender])
;; the filter predictate accepts a vector not a map
(sel ds :filter #(> (nth % 0) 25))
;; don't do this
(sel ds :filter #(> (nth % (.indexOf (:column-names ds) :age)) 25))

;; ($where query-map dataset)
;; alias for ...
;; (query-dataset dataset query-map)
;; there's a small query DSL for the query-map
($where {:gender :m} ds)
;; sticking with native clojure predicates is probably a better idea
($where #(> (:age %) 25) ds)

;; clojure.core
;; remove index
(filter #(> (:age %) 25) user-info)
;; keep index
(filter #(> (-> % val :age) 25) user-info)


;; ---- Selecting Columns ----

($ :age ds)
($ [:user-id :gender] ds)
($ [:not :lat :lon] ds)

;; clojure.core
(reduce #(conj %1 (:age %2))
        () user-info)
(reduce #(conj %1 ((juxt :gender :age) %2))
        () user-info)

;; ---- Create Views ----
(sel ($where {:gender :m} ds)
     :rows (range 4)
     :cols [:age :gender])

;; would be nice if we could bundle up a "view" into a single symbol ...

(defn flat-map [x] (interleave (keys x) (vals x)))

(defn create-view [config dataset]
  (let [{:keys [select where]} config]
    (apply (partial sel ($where where dataset))
           (flat-map select))))

(def query-config
  {:select {:rows (range 4)
            :cols [:age :gender]}
   :where #(> (:age %) 25)})

(def age-gender-query
  (create-view query-config ds))

(def age-gender-query-partial
  (partial create-view query-config))

;; now you can apply a query configuration to arbitrary datasets
(age-gender-query-partial ds)


;; ---- Applyiing functions to columns ----

(with-data ds
  [(mean ($ :age))
   (sd ($ :age))])

;; more concisely
((juxt mean sd) ($ :age ds))

;; clojure.core
((juxt mean sd) (reduce #(conj %1 (:age %2))
                        () user-info))


;; ---- Ordering Data ----

($order [:gender :age] :desc ds)

;; clojure.core
;; remove index
(sort-by :age user-info)
;; two level sort
(sort-by (juxt :gender :age) user-info)
;; keep index
(sort-by #((juxt :gender :age) (val %)) user-info-indexed)


;; ---- Apply a function to groups of rows ----

($rollup mean :age :gender ds)
;; applying lambdas
($rollup #(/ (apply + %) (count %)) :age :gender ds)

;; use the thread-last macro to compose functions
(->> ds
    ($rollup sd :activity :gender)
    ($order :activity :desc))


;; ---- Shaping Data ----

;; wide to long
(def long-fmt (deshape :group-by [:user-id]
                       :merge [:gender :age]
                       :data ds))

;; no native support in incanter for long to wide shaping ...
(defn shape
  [& {:keys [data row col value-var fill]
      :or {col :variable value-var :value}}]
  (to-dataset (vals (reduce (fn [res x]
                              (let [k (row x)
                                    var (col x)
                                    value (if (nil? (value-var x))
                                            fill
                                            (value-var x))]
                                (assoc res k (merge {row k
                                                     var value}
                                                    (res k)))))
                            {} (if (map? data)
                                 (:rows data)
                                 data)))))

(def wide-fmt (shape :data long-fmt
                     :row :user-id
                     :col :variable
                     :value-var :value))
