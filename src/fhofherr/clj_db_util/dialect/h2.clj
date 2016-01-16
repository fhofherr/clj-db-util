(ns fhofherr.clj-db-util.dialect.h2)

(defn get-generated-key
  [m]
  (get m (keyword "scope_identity()")))
