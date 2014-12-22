(ns fhofherr.clj-db-util.dialect.ast-test
  (:require [clojure.test :refer :all]
            [clojure.zip :as zip]
            [fhofherr.clj-db-util.dialect.ast :as ast]))

(deftest check-if-loc-is-rule
  (is (true? (ast/rule? (ast/zip [:RULE-NAME]))))
  (is (false? (ast/rule? (ast/zip ["_NAME"])))))

(deftest obtain-the-name-of-rule-that-lead-to-a-branch-in-the-ast
  (is (= :FROM (ast/get-rule (ast/zip [:FROM]))))
  (is (nil? (ast/get-rule (ast/zip ["_NAME"])))))

(deftest check-if-loc-is-token
  (is (true? (ast/token? (ast/zip [:RULE-NAME]))))
  (is (false? (ast/token? (ast/zip ["_NAME"]))))
  (is (false? (ast/token? (ast/zip [:RULE "VALUE"]))))
  (is (false? (ast/token? (zip/next (ast/zip [:RULE "VALUE"])))))
  (is (true? (ast/token? (zip/next (zip/next (ast/zip [:RULE "VALUE"])))))))

(deftest convert-token-to-string
  (is (= "RULE-NAME" (ast/token-to-str (ast/zip [:RULE-NAME]))))
  (is (= "VALUE"
         (ast/token-to-str (zip/next (zip/next (ast/zip [:RULE "VALUE"]))))))
  (is (= "REPLACEMENT" (ast/token-to-str (ast/zip [:RULE-NAME])
                                         {:RULE-NAME "REPLACEMENT"})))
  (is (nil? (ast/token-to-str (zip/next (ast/zip [:RULE "VALUE"])))))
  (is (nil? (ast/token-to-str (ast/zip ["_NAME"]))))
  (is (nil? (ast/token-to-str (ast/zip [:RULE "VALUE"])))))

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
        replacements {:PARAM "?" :EQ "="}]
    (is (= "SELECT * FROM dual WHERE 1 = ?"
           (ast/ast-to-str ast replacements)))))
