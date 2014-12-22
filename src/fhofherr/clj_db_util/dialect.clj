(ns fhofherr.clj-db-util.dialect
  (:refer-clojure :exclude [name])
  (:require [fhofherr.clj-db-util.dialect [h2 :as h2-dialect]
                                          [ast :as ast]]))

(def h2 {::name :h2
         ::parser h2-dialect/h2-parser
         ::replacements h2-dialect/replacements})

(defn parse
  [dialect sql-str]
  ((::parser dialect) sql-str))

(defn ast-to-str
  [dialect tree]
  (ast/ast-to-str tree (::replacements dialect)))
