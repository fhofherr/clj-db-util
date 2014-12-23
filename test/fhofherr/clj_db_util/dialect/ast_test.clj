(ns fhofherr.clj-db-util.dialect.ast-test
  (:require [clojure.test :refer :all]
            [clojure.zip :as zip]
            [fhofherr.clj-db-util.dialect.ast :as ast]))

(deftest check-if-loc-is-rule
  (is (true? (ast/rule? (ast/zip [:RULE-NAME]))))
  (is (false? (ast/rule? (ast/zip ["_NAME"]))))
  (is (false? (ast/rule? nil))))

(deftest check-for-certain-rule
  (is (true? (ast/rule= :RULE (ast/zip [:RULE]))))
  (is (false? (ast/rule= :RULE (ast/zip [:DIFFERENT-RULE]))))
  (is (false? (ast/rule= :RULE (ast/zip ["_NAME"]))))
  (is (false? (ast/rule= :RULE nil))))

(deftest obtain-the-name-of-rule-that-lead-to-a-branch-in-the-ast
  (is (= :FROM (ast/get-rule (ast/zip [:FROM]))))
  (is (nil? (ast/get-rule (ast/zip ["_NAME"])))))

(deftest find-the-loc-of-a-certain-rule
  (is (= [:RULE] (zip/node (ast/find-rule :RULE (ast/zip [:RULE])))))
  (is (= [:RULE] (zip/node (ast/find-rule :RULE (ast/zip [:PARENT
                                                          [:FIRST-CHILD]
                                                          [:SECOND-CHILD
                                                           [:RULE]]])))))
  (is (= [:RULE] (zip/node (ast/find-rule :RULE (ast/zip ["something"
                                                          [:RULE]])))))
  (is (nil? (ast/find-rule :RULE (ast/zip [:NOT-THERE]))))
  #_(is (nil? (ast/find-rule :RULE (ast/zip ["_NAME"])))))

(deftest check-if-loc-is-token
  (is (true? (ast/token? (ast/zip [:RULE-NAME]))))
  (is (false? (ast/token? (ast/zip ["_NAME"]))))
  (is (false? (ast/token? (ast/zip [:RULE "VALUE"]))))
  (is (false? (ast/token? (zip/next (ast/zip [:RULE "VALUE"])))))
  (is (true? (ast/token? (zip/next (zip/next (ast/zip [:RULE "VALUE"])))))))

(deftest convert-token-to-string
  (let [loc (ast/zip [:RULE-NAME])]
    (is (= ["RULE-NAME" (zip/next loc)] (ast/token-to-str loc))))
  (let [loc (zip/next (zip/next (ast/zip [:RULE "VALUE"])))]
    (is (= ["VALUE" (zip/next loc)]
           (ast/token-to-str loc))))
  (let [loc (zip/next (ast/zip [:RULE "VALUE"]))]
    (is (= [nil (zip/next loc)]
           (ast/token-to-str loc))))
  (let [loc (ast/zip ["_NAME"])]
    (is (= [nil (zip/next loc)]
           (ast/token-to-str loc))))
  (let [loc (ast/zip [:RULE "VALUE"])]
    (is (= [nil (zip/next loc)]
           (ast/token-to-str loc)))))

(deftest convert-ast-to-string
  (let [ast [:S
             [:DML
              [:SELECT-STMT
               [:SELECT]
               [:SELECT-EXPR "*"]
               [:FROM]
               [:TABLE-EXPR [:TABLE-NAME "dual"]]
               [:WHERE]
               [:INTEGRAL-NUMBER "1"]
               [:EQ]
               [:PARAM]]]]
        formatters {:PARAM "?" :EQ "="}]
    (is (= "SELECT * FROM dual WHERE 1 = ?"
           (ast/ast-to-str ast formatters)))))
