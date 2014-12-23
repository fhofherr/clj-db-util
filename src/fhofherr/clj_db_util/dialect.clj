(ns fhofherr.clj-db-util.dialect
  (:refer-clojure :exclude [name])
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [instaparse.core :as insta]
            [fhofherr.clj-db-util.dialect [h2 :as h2-dialect]
                                          [ast :as ast]]))

(def h2 {::name :h2
         ::resource-path "db/h2"
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

(defn statements-loc
  "Get the path where the dialect expects its statement resources.

  *Parameters*:

  - `dialect` the SQL dialect to use"
  [dialect]
  (str (::resource-path dialect) "/statements/"))

(defn load-statement
  "Load a SQL statement from a resource.

  The `stmt-path` is interpreted as a sub-path of the dialect's
  [[statements-loc]]. Returns the statement as a string or `nil` if
  no statement could be found.

  *Parameters*:

  - `dialect` the SQL dialect to use
  - `stmt-path` sub-path of the dialects [[statements-loc]]"
  [dialect stmt-path]
  (let [p (str (statements-loc dialect) stmt-path)
        r (io/resource p)]
    (if r
      (slurp r)
      (log/fatalf "Could not load statement '%s'" p))))
