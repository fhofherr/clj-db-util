(ns fhofherr.clj-db-util.core.transaction
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [fhofherr.clj-db-util.core.database :as database]))

(defrecord TransactionState [t-con])

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

(defn ^:dynamic *exception-occured*
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
         (*exception-occured* ~tx-state-bnd ex#)))))

(defn with-db-transaction
  [db tx-op]
  (jdbc/with-db-transaction
    [t-con (database/db-spec db)]
    (let [tx-state (map->TransactionState {:t-con t-con})
          [tx-result final-tx-state] (tx-op tx-state)]
      (if (rollback-only? final-tx-state)
        [tx-result err-transaction-rolled-back]
        [tx-result nil]))))

(defn transaction-state?
  [tx-state]
  (instance? TransactionState tx-state))
