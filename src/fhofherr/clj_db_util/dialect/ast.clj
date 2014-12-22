(ns fhofherr.clj-db-util.dialect.ast
  (:require [clojure.zip :as zip]))

(defn zip
  [root]
  (zip/vector-zip root))

(defn rule?
  [loc]
  (and (zip/branch? loc)
       (keyword? (first (zip/children loc)))))

(defn get-rule
  [loc]
  (when (rule? loc)
    (first (zip/children loc))))
