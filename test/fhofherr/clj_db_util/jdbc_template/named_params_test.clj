(ns fhofherr.clj-db-util.jdbc-template.named-params-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.jdbc-template.named-params :as np]))

(deftest extract-named-params
  (is (= [[:param-name] [:PARAM]]
         (np/extract-named-params [:NAMED-PARAM "param-name"])))
  (is (= [[:param-name] [:PARAM]]
         (np/extract-named-params [:NAMED-PARAM "PARAM-NAME"])))
  (let [ast [:PARENT
             [:NAMED-PARAM "PARAM1"]
             [:PARENT
              [:PARENT [:NAMED-PARAM "param2"]]
              [:NAMED-PARAM "param3"]]]
        expected-ast [:PARENT
                      [:PARAM]
                      [:PARENT
                       [:PARENT [:PARAM]]
                       [:PARAM]]]
        param-list [:param1 :param2 :param3]]
    (is (= [param-list expected-ast]
           (np/extract-named-params ast)))))
