(ns fhofherr.clj-db-util.transactions
  (:require [clojure.java.jdbc :as jdbc]))

;; Wrapper for a transaction. `op` is a function of two arguments returning
;; a map {::connection con ::value v}. The first argument to `op` is the
;; dialect currently in use, the second the connection.
(defrecord Transaction [op])

(alter-meta! #'->Transaction assoc :no-doc true)
(alter-meta! #'map->Transaction assoc :no-doc true)

;; Special value to mark a transaction as stopped
(def ^:private tx-stop {::connection nil ::value nil})

(defn tx-return
  "Take an arbitrary value `v` and wrap it into a transaction.

  *Parameters*:

  - `v` an arbitrary value to wrap into a transaction."
  [v]
  (Transaction. (fn [dialect con] {::connection con ::value v})))

(defn tx-bind
  "Take a transacton `tx` containting a value `v` and a function `f` of
  one argument returning another transaction. Apply `f` to `v` and return
  the resulting transaction.

  *Parameters*:

  - `tx` a transaction.
  - `f` a function expecting one argument and returning a transaction."
  [tx f]
  (Transaction. (fn [dialect con]
                  {:pre [dialect con]}
                  (let [{con* ::connection v ::value} ((:op tx) dialect con)
                        tx* (f v)]
                    (if con*
                      ((:op tx*) dialect con*)
                      tx-stop)))))

(defmacro deftx
  "Define a function that creates a transaction. The first entry of the
  definitions argument vector will be bound to the dialect used during the
  execution of the transaction. The second entry of the argument vector
  will be bound to the current database connection. Further arguments are
  optional."
  {:arglists '([fn-name [dialect db-spec & args] & body]
               [fn-name doc-string [dialect db-spec & args] & body])}
  [fn-name & body]
  (let [has-doc? (string? (first body))
        doc-string (if has-doc? (first body) nil)
        [args & decls] (if has-doc? (rest body) body)]
    (assert (symbol? fn-name)
            "Need a symbol as function name!")
    (assert (<= 2 (count args))
            "The first two arguments are needed for dialect and db-spec!")
    `(defn ~fn-name {:doc ~doc-string}
       [~@(drop 2 args)]
       (Transaction. (fn [~@(take 2 args)]
                       {::connection ~(second args)
                        ::value (do ~@decls)})))))

(defmacro deftx-
  "Same as [[deftx]] but yielding a non-public def."
  {:arglists '([fn-name [dialect db-spec & args] & body]
               [fn-name doc-string [dialect db-spec & args] & body])}
  [fn-name & body]
  `(deftx ~(with-meta fn-name (assoc (meta fn-name) :private true))
     ~@body))

(deftx tx-apply
  "Take the function `f` and apply it to the values `v` and `vs` within
  the context of a transaction."
  [dialect con f v & vs]
  (apply f v vs))

(defn tx-rollback
  "Rollback the transaction. Further steps won't be executed."
  []
  (Transaction. (fn [dialect con]
                  (jdbc/db-set-rollback-only! con)
                  tx-stop)))

(defn- emit-tx-step
  [[arg tx-expr]]
  (if tx-expr
    `(tx-bind (fn [~arg] ~tx-expr))
    `(tx-bind (fn [~arg] (tx-return ~arg)))))

(defn- emit-tx-steps
  [bindings]
  (reduce (fn [a s] (conj a (emit-tx-step s))) [] bindings))

(defmacro tx->
  "Evaluate transactions within a lexical context. The values of the
  transactions are bound to the given binding symbols. The last transaction
  in the bindings is returned as the evaluation result.

  *Examples*:

  ```clojure
  (tx-> _ (t/insert! :fruit {:name \"Apple\" :cost 2.99})
        _ (t/insert! :fruit_orders
                     {:fruit_id 1 :customer_name \"Fruit Sales Inc.\"})
  ```"
  [& bindings]
  (assert (even? (count bindings))
          "Need an even number of bindings!")
  (let [args (->> bindings
               (partition 2)
               (map first))
        exprs (->> bindings
                (partition 2)
                (map second))
        first-tx-expr (first exprs)
        other-tx-exprs (-> exprs
                           (rest)
                           (lazy-cat (repeat nil)))
        reordered-bindings (partition 2 (interleave args other-tx-exprs))]
    `(-> ~first-tx-expr
         ~@(emit-tx-steps reordered-bindings))))

(defn tx-exec
  "Execute the transaction `tx` in the database represented by `db-spec`.
  The `dialect` is added for convenience since some database functions need it
  to perform their tasks (this might not be a good idea, in which case I'll
  remove the dialect)."
  [dialect db-spec tx]
  (jdbc/with-db-transaction [con db-spec]
    (::value ((:op tx) dialect con))))

(defmacro tx-exec->
  "Like [[tx->]] but immediately call [[tx-exec]] on the result."
  [dialect db-spec & bindings]
  `(tx-exec ~dialect ~db-spec (tx-> ~@bindings)))
