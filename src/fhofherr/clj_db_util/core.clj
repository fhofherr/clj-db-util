(ns fhofherr.clj-db-util.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log])
  (:import (javax.sql DataSource)
           (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariDataSource HikariConfig)))

(defrecord Database [^DataSource datasource
                     ^String db-resource-path
                     ^String schema
                     ^String vendor
                     ^Flyway migrator])

(alter-meta! #'->Database assoc :no-doc true)
(alter-meta! #'map->Database assoc :no-doc true)

(defn migrations-loc [{:keys [db-resource-path schema vendor]}]
  {:pre [db-resource-path schema vendor]}
  (format "%s/%s/%s/migrations" db-resource-path (name vendor) schema))

(defn statements-loc [{:keys [db-resource-path schema vendor]}]
  {:pre [db-resource-path vendor schema]}
  (format "%s/%s/%s/statements" db-resource-path (name vendor) schema))

(defn- vendor-from-url
  [^String url]
  (cond
    (.startsWith url "jdbc:h2") :h2
    (.startsWith url "jdbc:postgresql") :postgres
    :else (throw (ex-info (format "Can't determine vendor for database url: %s" url)
                          {:cause #{:unknown-database-vendor}
                           :url url}))))

(defn- add-migrator
  [db]
  {:pre [(:datasource db)]}
  (let [fw (doto
            (Flyway.)
             (.setDataSource (:datasource db))
             (.setLocations (into-array String [(migrations-loc db)])))
        fw (if (= (:schema db) "default")
             fw
             (doto fw
               (.setSchemas (into-array String [(:schema db)]))))]
    (assoc db :migrator fw)))

(defn connect-to-db
  [^String url ^String user ^String password]
  {:pre [url user password]}
  (let [cfg (doto (HikariConfig.)
              (.setJdbcUrl url)
              (.setUsername user)
              (.setPassword password))
        ds (HikariDataSource. cfg)]
    (let [db-cfg {:db-resource-path "classpath:/db"
                  :schema "default"
                  :datasource ds
                  :vendor (vendor-from-url url)}]
      (-> db-cfg
          (map->Database)
          (add-migrator)))))

(defn disconnect-from-db [{:keys [datasource]}]
  {:pre [datasource]}
  (.close datasource))

(defmacro with-database
  [[db-sym connect-expr :as bnd] & body]
  {:pre [(symbol? db-sym) connect-expr]}
  `(let ~bnd
     (try
       ~@body
       (finally
         (disconnect-from-db ~db-sym)))))

(defn verify-connection
  [db]
  {:pre [db]}
  (let [c  (.getConnection ^DataSource (:datasource db))]
    (try
      (.isValid c 5)
      (finally
        (.close c)))))

(defn db-info
  [{:keys [migrator]}]
  {:pre [migrator]}
  (.info migrator))

(defn db-version
  [db]
  (some-> db
          (db-info)
          (.current)
          (.getVersion)
          (.getVersion)))

(defn migrate-db
  [{:keys [migrator] :as db}]
  {:pre [migrator]}
  (.migrate migrator)
  (db-version db))

(defn clean-db
  [{:keys [migrator]}]
  (.clean migrator))

(defn db-spec
  [{:keys [datasource]}]
  {:pre [datasource]}
  {:datasource datasource})

(defrecord TransactionState [t-con])
(alter-meta! #'->TransactionState assoc :no-doc true)
(alter-meta! #'map->TransactionState assoc :no-doc true)

(defn transaction-state?
  [tx-state]
  (instance? TransactionState tx-state))

(def err-transaction-rolled-back {:error "Transaction rolled back"})

(defn rollback-only?
  [{:keys [t-con]}]
  {:pre [t-con]}
  (jdbc/db-is-rollback-only t-con))

(defn set-rollback-only!
  [{:keys [t-con] :as tx-state}]
  {:pre [t-con]}
  (jdbc/db-set-rollback-only! t-con)
  tx-state)

(defn ^:dynamic *exception-during-transaction*
  [tx-state ^Exception ex]
  (log/warn ex "Exception occured during transaction")
  (let [rolled-back (set-rollback-only! tx-state)]
    [nil rolled-back]))

(defmacro transactional-operation
  [[tx-state-bnd] & body]
  {:pre [(symbol? tx-state-bnd)]}
  ;; TODO add meta data identifying the function as transactional operation?
  `(fn [~tx-state-bnd]
     (try
       (let [res# ~@body]
         [res# ~tx-state-bnd])
       (catch Exception ex#
         (*exception-during-transaction* ~tx-state-bnd ex#)))))

(defn transactional
  [value])

(defmacro transactional-let
  [bindings & body])

(defn insert!
  [table & records])

(defn delete!
  [table condition])

(defn query-str
  [stmt-str])

(defn with-db-transaction
  [db tx-op]
  (jdbc/with-db-transaction
    [t-con (db-spec db)]
    (let [tx-state (map->TransactionState {:t-con t-con})
          [tx-result final-tx-state] (tx-op tx-state)]
      (if (rollback-only? final-tx-state)
        [tx-result err-transaction-rolled-back]
        [tx-result nil]))))