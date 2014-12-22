(ns fhofherr.clj-db-util.jdbc-template.named-params
  (:require [clojure.zip :as zip]
            [fhofherr.clj-db-util.dialect.ast :as ast]))

(defn- do-extract
  [[res loc]]
  (if (and (ast/rule? loc)
           (= :NAMED-PARAM (ast/get-rule loc)))
    (let [param-name (-> loc
                         (zip/children)
                         (second)
                         (clojure.string/lower-case)
                         (keyword))
          next-loc (zip/next (zip/replace loc [:PARAM]))]
      [(conj res param-name) next-loc])
    [res (zip/next loc)]))

(defn extract-named-params
  [tree]
  (let [[res loc] (->> [[] (ast/zip tree)]
                       (iterate do-extract)
                       (drop-while (fn [[_ l]] (not (zip/end? l))))
                       (first))]
    [res (zip/root loc)]))
