(ns fhofherr.clj-db-util.jdbc-template
  (:require [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [fhofherr.clj-db-util.db-connection :as db-con]
            [fhofherr.clj-db-util.jdbc-template [ast :as ast]
                                                [parser :as parser]]
            [fhofherr.clj-db-util.dialect :as d]
            [fhofherr.clj-db-util.transactions :as tx]
            [fhofherr.clj-db-util.jdbc-template [template-vars :as tv]
                                                [named-params :as np]]))

(defn- execute-sql-str
  [f db sql-str options]
  (when (empty? sql-str)
    (throw (IllegalArgumentException. "No sql-str given!")))
  (let [[argv tree] (->> sql-str
                         (parser/parse)
                         (tv/process-template-vars (:template-vars options {}))
                         (np/process-named-params (:params options {})))
        stmt (ast/ast-to-str tree)
        jdbc-opts (-> options
                   (dissoc :template-vars)
                   (dissoc :params)
                   (seq)
                   (flatten))]
    (if-not (empty? stmt)
      (do
        (log/infof "Executing query: '%s'" stmt)
        (apply f
               (db-con/db-spec db)
               (into [stmt] argv)
               jdbc-opts))
      (throw (IllegalArgumentException. "Could not parse query!")))))

(tx/deftx query
  [db sql-str & {:as options}]
  (execute-sql-str jdbc/query db sql-str options))

(tx/deftx load-statement
  "Load a statement from a resource file using the database's dialect."
  [db stmt-path]
  (d/load-statement (db-con/dialect db) stmt-path))

(tx/deftx insert!
  "Wrapper around `clojure.java.jdbc/insert!`.

  Delegates to `clojure.java.jdbc/insert!` internally and supports everything
  `clojure.java.jdbc/insert!` does:

  - Insertion of single rows
  - Insertion of multiple rows using multiple inserts
  - Insertion of multiple rows using a single insert

  **Unified extraction of generated keys**

  If the SQL dialect supports it and either a single row or multiple rows using
  multiple inserts were inserted a sequence of generated keys will be returned.
  If multiple rows are inserted using a single insert the total number of rows
  inserted will be returned."
  [db table & options]
  (let [xs (apply jdbc/insert! (db-con/db-spec db) table options)]
    (if (map? (first options)) ;; Check if we are inserting row maps
      (d/get-generated-keys (db-con/dialect db) xs)
      (count xs))))
