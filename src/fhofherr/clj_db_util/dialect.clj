(ns fhofherr.clj-db-util.dialect
  (:refer-clojure :exclude [name])
  (:require [clojure.tools.logging :as log]
            [instaparse.core :as insta]
            [fhofherr.clj-db-util.dialect [h2 :as h2-dialect]
                                          [ast :as ast]]))

(def h2 {::name :h2
         ::parser h2-dialect/h2-parser
         ::replacements h2-dialect/replacements})

(defn parse
  "Use the `dialect`'s parser to parse the `sql-str`.

  Return the AST representation of the statement if parsing was successful, or
  `nil` if a parse error occured.

  *Parameters*:

  - `dialect` the SQL dialect to use
  - `sql-str` the SQL string to parse."
  [dialect sql-str]
  (let [ast ((::parser dialect) sql-str)]
    (if (insta/failure? ast)
      (log/fatal (insta/get-failure ast))
      ast)))

(defn ast-to-str
  [dialect tree]
  (ast/ast-to-str tree (::replacements dialect)))
