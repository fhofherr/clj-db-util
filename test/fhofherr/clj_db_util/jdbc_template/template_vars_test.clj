(ns fhofherr.clj-db-util.jdbc-template.template-vars-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.jdbc-template.template-vars :as tv]))

(deftest process-template-vars

  (testing "variable replacement"
    (is (= [:SQL_TOKEN "PUBLIC"]
           (tv/process-template-vars {:schema "PUBLIC"}
                                     [:TEMPLATE_VAR "schema"])))

    (is (= [:SQL_TOKEN "PUBLIC"]
           (tv/process-template-vars {:schema "PUBLIC"}
                                     [:TEMPLATE_VAR "SCHEMA"])))

    (is (= [:SQL_TOKEN "1"]
           (tv/process-template-vars {:number 1}
                                     [:TEMPLATE_VAR "number"])))

    (is (nil? (tv/process-template-vars {}
                                        [:TEMPLATE_VAR "schema"])))

    (is (nil? (tv/process-template-vars {:schema ";invalid"} 
                                        [:TEMPLATE_VAR "schema"])))))
