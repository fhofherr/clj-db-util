(ns fhofherr.clj-db-util.jdbc-template
  (:require [clojure.java.jdbc :as jdbc]))

(defn query-str
  [dialect db-spec sql-str & {:keys [result-set-fn]
                              :or {:result-set-fn identity}}]
  (jdbc/query db-spec [sql-str] :result-set-fn result-set-fn))
