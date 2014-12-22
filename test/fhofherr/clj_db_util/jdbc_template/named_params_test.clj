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

(deftest make-argv
  (is (= ["value1" "value2" "value3"]
         (np/make-argv [:param1 :param2 :param3]
                       {:param1 "value1"
                        :param2 "value2"
                        :param3 "value3"}))))
