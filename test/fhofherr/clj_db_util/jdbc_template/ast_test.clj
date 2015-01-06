(ns fhofherr.clj-db-util.jdbc-template.ast-test
  (:require [clojure.test :refer :all]
            [clojure.zip :as zip]
            [fhofherr.clj-db-util.jdbc-template.ast :as ast]))

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
