(ns fhofherr.clj-db-util.jdbc-template
  (:require [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [fhofherr.clj-db-util.db :as db-con]
            [fhofherr.clj-db-util.dialect :as d]
            [fhofherr.clj-db-util.transactions :as tx]
            [fhofherr.clj-db-util.jdbc-template [template-vars :as tv]
                                                [named-params :as np]]))

(tx/deftx query-str
  [db sql-str & {:keys [params
                        template-vars
                        result-set-fn]
                 :or {:params {}
                      :template-vars {}
                      :result-set-fn doall}}]
  (let [[argv tree] (->> sql-str
                         (d/parse (db-con/dialect db))
                         (tv/process-template-vars (db-con/dialect db) template-vars)
                         (np/process-named-params params))
        stmt (d/ast-to-str (db-con/dialect db) tree)]
    (if (not-empty stmt)
      (do
        (log/infof "Executing query: '%s'" stmt)
        (jdbc/query (db-con/db-spec db)
                    (into [stmt] argv)
                    :result-set-fn result-set-fn))
      (log/error "Query was empty!"))))

(defn query-res
  [dialect stmt-path & options]
  (let [sql-str (d/load-statement dialect stmt-path)]
    (tx/tx-bind (tx/tx-return sql-str)
                #(apply query-str % options))))

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
