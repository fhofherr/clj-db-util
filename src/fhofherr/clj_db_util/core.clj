(ns fhofherr.clj-db-util.core
  (:require [fhofherr.clj-db-util.core.database :as database]))

(defn db-spec
  [url user password]
  {:pre [url user password]}
  {:datasource (database/datasource url user password)})

(defn verify-connection
  [db-spec]
  {:pre [db-spec]}
  (let [c (-> (:datasource db-spec)
              (.getConnection))]
    (try
      (.isValid c 5)
      (finally
        (.close c)))))
