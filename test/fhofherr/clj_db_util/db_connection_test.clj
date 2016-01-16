(ns fhofherr.clj-db-util.db-connection-test
  (:require [clojure.test :refer :all]
            [clojure.string :as s]
            [fhofherr.clj-test-util.core :refer :all]
            [fhofherr.clj-db-util.db-connection :as db-con]
            [fhofherr.clj-db-util.jdbc-template :as t]
            [fhofherr.clj-db-util.transactions :refer [tx-exec]]
            [fhofherr.clj-db-util.support.test-db :as test-db]))

(defn global-conversion
  [x]
  (s/replace x #"_global" ""))

(fixture globally-modified-conversions
         [:around-each (test-db/prepare-db test-db/h2-in-memory
                                           :identifiers s/upper-case
                                           :entities global-conversion)]

         (deftest identifier-conversion

           (testing "convert identifiers using the global conversion"
             (is (= {:VALUE 1}
                    (tx-exec test-db/*db*
                             (t/query "select 1 as value from dual;"
                                      :result-set-fn first)))))

           (testing "locally override identifier conversion"
             (is (= {:value 1}
                    (tx-exec test-db/*db*
                             (t/query "select 1 as value from dual;"
                                      :result-set-fn first
                                      :identifiers clojure.string/lower-case))))))

         (deftest entity-conversion

           (testing "convert entity names using the global conversion"
             (do (tx-exec test-db/*db*
                          (t/insert! :fruit_global {:name "Apple" :cost 2.99}))
                 (is (= {:ID 1 :NAME "Apple" :COST 2.99M}
                        (tx-exec test-db/*db*
                                   (t/query "select *
                                            from fruit
                                            where name = 'Apple'"
                                            :result-set-fn first))))))

           (testing "override global entity conversions"
             (do (tx-exec test-db/*db*
                          (t/insert! :fruit_local
                                     {:name "Apple" :cost 2.99}
                                     :entities #(s/replace % #"_local" "")))
                 (is (= {:ID 1 :NAME "Apple" :COST 2.99M}
                        (tx-exec test-db/*db*
                                   (t/query "select *
                                            from fruit
                                            where name = 'Apple'"
                                            :result-set-fn first))))))))
