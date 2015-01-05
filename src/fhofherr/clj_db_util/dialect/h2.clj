(ns fhofherr.clj-db-util.dialect.h2
  (:require [clojure.java.io :refer [resource]]
            [clojure.zip :as zip]
            [instaparse.core :as insta]
            [fhofherr.clj-db-util.dialect.ast :as ast]))

(def h2-parser (-> "fhofherr/clj-db-util/grammars/h2.bnf"
                   (resource)
                   (insta/parser)))

(defn get-generated-key
  [m]
  (get m (keyword "scope_identity()")))

(defn- fmt-table-expr
  [loc]
  (if-let [schema-name-loc (ast/find-rule :SCHEMA-NAME loc)]
    (let [[_ schema] (zip/node schema-name-loc)
          [_ table] (zip/node (ast/find-rule :TABLE-NAME loc))]
      [(str schema "." table) (zip/right loc)])
    [nil (zip/next loc)]))

(defn- fmt-string
  [loc]
  (let [[_ ldelim s rdelim] (zip/node loc)]
    [(str ldelim s rdelim) (zip/right loc)]))

(def replacements {:CONCAT "||"
                   :EQ "="
                   :GEQ ">="
                   :GT ">"
                   :LEQ "<="
                   :LT "<"
                   :NEQ "<>"
                   :PARAM "?"

                   :TABLE-EXPR fmt-table-expr
                   :STRING fmt-string

                   :DISTINCT-FROM "DISTINCT FROM"
                   :NEXT-VALUE-FOR "NEXT VALUE FOR"
                   :OVERLAPPING "&&"})
