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
  (is (true? (ast/token? (zip/next (ast/zip [:RULE "VALUE"]))))))
