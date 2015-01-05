(ns fhofherr.clj-db-util.migrations-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.support.test-db :as test-db]
            [fhofherr.clj-db-util.jdbc-template :as t]
            [fhofherr.clj-db-util.dialect :refer [h2]]
            [fhofherr.clj-db-util.migrations :as migs]
            [fhofherr.clj-db-util.transactions :refer [tx-exec]]))

(def placeholder-value "THE VALUE")

(use-fixtures :each (test-db/prepare-db h2
                                        test-db/h2-in-memory
                                        :schema "MIG_TEST"
                                        :placeholders {:placeholder
                                                       placeholder-value}))

(deftest schema-support
  (is (= [1]
         (tx-exec test-db/*dialect*
                  test-db/*db-spec*
                  (t/insert! :MIG_TEST.vegetables {:name "Cucumber"})))))

(deftest placeholder-support
  (is (= {:placeholder_value placeholder-value}
         (tx-exec test-db/*dialect*
                  test-db/*db-spec*
                  (t/query-str 
                    "SELECT placeholder_value 
                    FROM MIG_TEST.placeholder_support
                    WHERE placeholder_value = :value"
                    :params {:value placeholder-value}
                    :result-set-fn first)))))

(deftest executes-callbacks
  (let [callbacks (atom [])]
    (letfn [(reset-callbacks [] (reset! callbacks []))
            (after-migrate [con] (swap! callbacks conj :after-migrate ))
            (before-migrate [con] (swap! callbacks conj :before-migrate ))
            (after-clean [con] (swap! callbacks conj :after-clean ))
            (before-clean [con] (swap! callbacks conj :before-clean ))]

      (testing "migration callbacks"

        (do (reset-callbacks)
            (migs/migrate test-db/*dialect*
                          test-db/*db-spec*
                          :callbacks [{:before-migrate [before-migrate]}
                                      {:after-migrate [after-migrate]}])
            (is (= [:before-migrate :after-migrate] @callbacks)))

        (do (reset-callbacks)
            (migs/migrate test-db/*dialect*
                          test-db/*db-spec*
                          :callbacks [{:before-migrate [before-migrate]
                                       :after-migrate [after-migrate]}])
            (is (= [:before-migrate :after-migrate] @callbacks)))

        (do (reset-callbacks)
            (migs/migrate test-db/*dialect*
                          test-db/*db-spec*
                          :callbacks {:before-migrate [before-migrate]
                                      :after-migrate [after-migrate]})
            (is (= [:before-migrate :after-migrate] @callbacks)))

        (do (reset-callbacks)
            (migs/migrate test-db/*dialect*
                          test-db/*db-spec*
                          :callbacks {:before-migrate before-migrate
                                      :after-migrate after-migrate})
            (is (= [:before-migrate :after-migrate] @callbacks)))
        )

      (testing "clean callbacks"

        (do (reset-callbacks)
            (migs/clean test-db/*dialect*
                        test-db/*db-spec*
                        :callbacks [{:before-clean [before-clean]}
                                    {:after-clean [after-clean]}])
            (is (= [:before-clean :after-clean] @callbacks)))))))
