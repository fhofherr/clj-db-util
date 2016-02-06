(ns fhofherr.clj-db-util.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [fhofherr.clj-db-util.core.named-params :as named-params])
  (:import (javax.sql DataSource)
           (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariDataSource HikariConfig)
           (org.flywaydb.core.api MigrationInfoService)
           (clojure.lang Keyword)))

(defrecord Database [^DataSource datasource
                     ^String db-resource-path
                     ^String schema
                     ^Keyword vendor
                     ^Flyway migrator])

(alter-meta! #'->Database assoc :no-doc true)
(alter-meta! #'map->Database assoc :no-doc true)

(defn database?
  [db]
  (instance? Database db))

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

(defn disconnect-from-db [{:keys [^HikariDataSource datasource]}]
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

(defn ^MigrationInfoService db-info
  [{:keys [^Flyway migrator]}]
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
  [{:keys [^Flyway migrator] :as db}]
  {:pre [migrator]}
  (.migrate migrator)
  (db-version db))

(defn clean-db
  [{:keys [^Flyway migrator]}]
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
  {:pre [t-con (transaction-state? tx-state)]}
  (jdbc/db-set-rollback-only! t-con)
  tx-state)

(defn ^:dynamic *exception-during-transaction*
  [tx-state ^Exception ex]
  (log/warn ex "Exception occured during transaction")
  ;; TODO repeatedly call and log getNextException (if possible) (see java.sql.BatchUpdateException)
  (let [rolled-back (set-rollback-only! tx-state)]
    [nil rolled-back]))

(defmacro transactional-operation
  [[tx-state-bnd] & body]
  {:pre [(symbol? tx-state-bnd) (not-empty body)]}
  ;; TODO add meta data identifying the function as transactional operation?
  `(fn [~tx-state-bnd]
     {:post [(sequential? ~'%) (transaction-state? (second ~'%))]}
     (let [tx-state# ~tx-state-bnd]
       (try
         (if (rollback-only? tx-state#)
           [nil tx-state#]
           (io! ~@body))
         (catch Exception ex#
           (*exception-during-transaction* tx-state# ex#))))))

(defmacro transactional
  [form]
  `(transactional-operation [tx-state#] [~form tx-state#]))

(defn transactional-bind
  [tx-op tx-op-factory]
  (transactional-operation
   [tx-state]
   (let [[res next-tx-state] (tx-op tx-state)
         next-tx-op (tx-op-factory res)]
     (next-tx-op next-tx-state))))

(defn- emit-tx-op-factory
  [bnd tx-op]
  `(fn [~bnd] ~tx-op))

(defmacro transactional-let
  [bindings & body]
  {:pre [(not-empty bindings) (even? (count bindings)) (not-empty body)]}
  (let [emit-binding-op-factoy (fn [[tx-op op-factory] [bnd-sym prev-tx-op]]
                                 (let [prev-op-factory (emit-tx-op-factory bnd-sym
                                                                           `(transactional-bind ~tx-op ~op-factory))]
                                   [prev-tx-op prev-op-factory]))
        reversed-bindings (->> bindings
                               (partition 2)
                               (reverse))
        [innermost-bnd-sym innermost-tx-op] (first reversed-bindings)
        innermost-op-factory (emit-tx-op-factory innermost-bnd-sym `(transactional ~@body))
        [outermost-tx-op outermost-op-factory] (reduce emit-binding-op-factoy
                                                       [innermost-tx-op innermost-op-factory]
                                                       (rest reversed-bindings))]
    `(transactional-bind ~outermost-tx-op ~outermost-op-factory)))

(defn- prepare-params
  [s param-vals]
  (if (map? param-vals)
    (let [{:keys [parsed-str param-keys]} (named-params/parse-str s)
          mapped-vals (map #(get param-vals %) param-keys)]
      (concat [parsed-str] mapped-vals))
    (concat [s] (seq param-vals))))

(defn- wrap-jdbc-fn
  [jdbc-fn & args]
  (transactional-operation
   [tx-state]
   (let [res (apply jdbc-fn (:t-con tx-state) args)]
     [res tx-state])))

(defn insert!
  [table & records]
  {:pre [table (not-empty records)]}
  (apply wrap-jdbc-fn jdbc/insert! table records))

(defn update!
  ([table value]
   (update! table value nil))
  ([table value where-clause]
   (update! table value where-clause nil))
  ([table value where-clause param-vals]
   {:pre [table value]}
   (wrap-jdbc-fn jdbc/update! table value (prepare-params where-clause param-vals))))

(defn delete!
  ([table]
   (delete! table nil))
  ([table where-clause]
   (delete! table where-clause nil))
  ([table where-clause param-vals]
   {:pre [table]}
   (wrap-jdbc-fn jdbc/delete! table (prepare-params where-clause param-vals))))

(defn query
  ([sql-str]
   (query sql-str nil))
  ([sql-str param-vals]
   {:pre [sql-str]}
   (wrap-jdbc-fn jdbc/query (prepare-params sql-str param-vals))))

(defn execute-str!
  ([sql-str]
   (execute-str! sql-str nil))
  ([sql-str param-vals]
   {:pre [sql-str]}
   (wrap-jdbc-fn jdbc/execute! (prepare-params sql-str param-vals))))

(defn rollback!
  []
  (transactional-operation
   [tx-state]
   [nil (set-rollback-only! tx-state)]))

(defn with-db-transaction
  [db tx-op]
  {:pre [(database? db)]}
  (io!
   (jdbc/with-db-transaction
     [t-con (db-spec db)]
     (let [tx-state (map->TransactionState {:t-con t-con})
           [tx-result final-tx-state] (tx-op tx-state)]
       (if (rollback-only? final-tx-state)
         [tx-result err-transaction-rolled-back]
         [tx-result nil])))))