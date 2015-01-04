(ns fhofherr.clj-db-util.migrations-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.support.test-db :as test-db]
            [fhofherr.clj-db-util.jdbc-template :as t]
            [fhofherr.clj-db-util.dialect :refer [h2]]))

(use-fixtures :each (test-db/prepare-db h2
                                        test-db/h2-in-memory
                                        :schema "MIG_TEST"))

(deftest schema-support
  (is (= [1]
         (t/insert! test-db/*dialect*
                    test-db/*db-spec*
                    :MIG_TEST.vegetables
                    {:name "Cucumber"}))))
