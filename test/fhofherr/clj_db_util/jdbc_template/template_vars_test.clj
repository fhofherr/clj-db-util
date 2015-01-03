(ns fhofherr.clj-db-util.jdbc-template.template-vars-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.dialect :as d]
            [fhofherr.clj-db-util.jdbc-template.template-vars :as tv]))

(deftest process-template-vars

  (testing "variable replacement"
    (is (= [:SCHEMA-NAME "PUBLIC"]
           (tv/process-template-vars d/h2
                                     {:schema "PUBLIC"} 
                                     [:SCHEMA-NAME
                                      [:TEMPLATE-VAR
                                       "{{"
                                       [:TEMPLATE-VAR-NAME "schema"]
                                       "}}"]])))
    (is (= [:SCHEMA-NAME "PUBLIC"]
           (tv/process-template-vars d/h2
                                     {:schema "PUBLIC"} 
                                     [:SCHEMA-NAME
                                      [:TEMPLATE-VAR
                                       "{{"
                                       [:TEMPLATE-VAR-NAME "SCHEMA"]
                                       "}}"]])))
    (is (nil? (tv/process-template-vars d/h2
                                        {} 
                                        [:SCHEMA-NAME
                                         [:TEMPLATE-VAR
                                          "{{"
                                          [:TEMPLATE-VAR-NAME "SCHEMA"]
                                          "}}"]])))
    (is (nil? (tv/process-template-vars d/h2
                                        {:schema "089-invalid"} 
                                        [:SCHEMA-NAME
                                         [:TEMPLATE-VAR
                                          "{{"
                                          [:TEMPLATE-VAR-NAME "SCHEMA"]
                                          "}}"]])))))
