(ns fhofherr.clj-db-util.jdbc-template
  (:require [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [fhofherr.clj-db-util.dialect :as d]
            [fhofherr.clj-db-util.jdbc-template.named-params :as np]))

(defn query-str
  [dialect db-spec sql-str & {:keys [params
                                     result-set-fn]
                              :or {:params {}
                                   :result-set-fn identity}}]
  (let [[ps tree] (-> (d/parse dialect sql-str)
                      (np/extract-named-params))
        argv (np/make-argv ps params)
        stmt (d/ast-to-str dialect tree)]
    (if (not-empty stmt)
      (do
        (log/infof "Executing query: '%s'" stmt)
        (jdbc/query db-spec
                    (into [stmt] argv)
                    :result-set-fn result-set-fn))
      (log/error "Query was empty!"))))
