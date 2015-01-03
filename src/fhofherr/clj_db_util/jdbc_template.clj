(ns fhofherr.clj-db-util.jdbc-template
  (:require [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [fhofherr.clj-db-util.dialect :as d]
            [fhofherr.clj-db-util.jdbc-template [template-vars :as tv]
                                                [named-params :as np]]))

(defn query-str
  [dialect db-spec sql-str & {:keys [params
                                     template-vars
                                     result-set-fn]
                              :or {:params {}
                                   :template-vars {}
                                   :result-set-fn identity}}]
  (let [[argv tree] (->> sql-str
                         (d/parse dialect)
                         (tv/process-template-vars dialect template-vars)
                         (np/process-named-params params))
        stmt (d/ast-to-str dialect tree)]
    (if (not-empty stmt)
      (do
        (log/infof "Executing query: '%s'" stmt)
        (jdbc/query db-spec
                    (into [stmt] argv)
                    :result-set-fn result-set-fn))
      (log/error "Query was empty!"))))

(defn query-res
  [dialect db-spec stmt-path & {:keys [params
                                       result-set-fn]
                                :or {:params {}
                                     :result-set-fn identity}}]
  (let [sql-str (d/load-statement dialect stmt-path)]
    (query-str dialect
               db-spec
               sql-str
               :params params
               :result-set-fn result-set-fn)))
