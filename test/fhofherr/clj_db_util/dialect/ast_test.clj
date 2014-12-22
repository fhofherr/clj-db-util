(ns fhofherr.clj-db-util.dialect.ast-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.dialect.ast :as ast]))

(deftest check-if-loc-is-rule
  (is (true? (ast/rule? (ast/zip [:RULE-NAME]))))
  (is (false? (ast/rule? (ast/zip ["_NAME"])))))

(deftest obtain-the-name-of-rule-that-lead-to-a-branch-in-the-ast
  (is (= :FROM (ast/get-rule (ast/zip [:FROM]))))
  (is (nil? (ast/get-rule (ast/zip ["_NAME"])))))
