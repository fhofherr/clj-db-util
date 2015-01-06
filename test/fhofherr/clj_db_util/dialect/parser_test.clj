(ns fhofherr.clj-db-util.dialect.parser-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.dialect.parser :as parser]))

(deftest plain-statements
  (is (= [:S
          [:STMT
           [:TOKEN [:SQL_TOKEN "SELECT"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "*"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "FROM"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "dual"]]]]
         (parser/parse "SELECT * FROM dual")))

  (is (= [:S
          [:STMT
           [:TOKEN [:SQL_TOKEN "SELECT"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "$$" "Hello World" "$$"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "as"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "result"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "FROM"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "dual"]]]]
         (parser/parse "SELECT $$Hello World$$ as result FROM dual")))

  (is (= [:S
          [:STMT
           [:TOKEN [:SQL_TOKEN "SELECT"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "'" "name" "'"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "as"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "result"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "FROM"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "dual"]]]]
         (parser/parse "SELECT 'name' as result FROM dual")))

  (is (= [:S
          [:STMT
           [:TOKEN [:SQL_TOKEN "SELECT"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "1"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "FROM"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "dual"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "WHERE"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "1"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "="]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "1"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "AND"]]
           [:SEPARATOR " "]
           [:TOKEN [:SQL_TOKEN "EXISTS"]]
           [:SEPARATOR " "]
           [:TOKEN "("
            [:STMT
             [:TOKEN [:SQL_TOKEN "SELECT"]]
             [:SEPARATOR " "]
             [:TOKEN [:SQL_TOKEN "1"]]
             [:SEPARATOR " "]
             [:TOKEN [:SQL_TOKEN "FROM"]]
             [:SEPARATOR " "]
             [:TOKEN [:SQL_TOKEN "DUAL"]]
             [:SEPARATOR " "]
             [:TOKEN [:SQL_TOKEN "WHERE"]]
             [:SEPARATOR " "]
             [:TOKEN [:SQL_TOKEN "2"]]
             [:SEPARATOR " "]
             [:TOKEN [:SQL_TOKEN "="]]
             [:SEPARATOR " "]
             [:TOKEN [:SQL_TOKEN "2"]]]
            ")"]]]
         (parser/parse "SELECT 1 FROM dual WHERE 1 = 1 AND EXISTS (SELECT 1 FROM DUAL WHERE 2 = 2)"))))

(deftest parse-dollar-quoted-sql-token
  (is (= [:SQL_TOKEN "$$" "$$"]
         (parser/parse "$$$$" :start :SQL_TOKEN)))

  (is (= [:SQL_TOKEN "$$" "$" "$$"]
         (parser/parse "$$$$$" :start :SQL_TOKEN)))

  (is (= [:SQL_TOKEN "$$" "Hello World" "$$"]
         (parser/parse "$$Hello World$$" :start :SQL_TOKEN)))

  (is (= [:SQL_TOKEN "$$" "This costs 15$" "$$"]
         (parser/parse "$$This costs 15$$$" :start :SQL_TOKEN)))

  (is (= [:SQL_TOKEN "$$" "$15 is too mutch" "$$"]
         (parser/parse "$$$15 is too mutch$$" :start :SQL_TOKEN)))

  (is (= [:SQL_TOKEN "$$" "15$ is a fair price" "$$"]
         (parser/parse "$$15$ is a fair price$$" :start :SQL_TOKEN))))

(deftest parse-single-quoted-sql-token
  (is (= [:SQL_TOKEN "'" "'"] (parser/parse "''" :start :SQL_TOKEN)))
  (is (= [:SQL_TOKEN "'" "something" "'"] (parser/parse "'something'"
                                                        :start :SQL_TOKEN)))
  (is (= [:SQL_TOKEN "'" "John''s car." "'"] (parser/parse "'John''s car.'"
                                                           :start :SQL_TOKEN))))
(deftest parse-double-quoted-sql-token
  (is (= [:SQL_TOKEN "\"" "NAME" "\""]
         (parser/parse "\"NAME\"" :start :SQL_TOKEN)))
  (is (= [:SQL_TOKEN "\"" "name" "\""]
         (parser/parse "\"name\"" :start :SQL_TOKEN)))
  (is (= [:SQL_TOKEN "\"" "14 name" "\""]
         (parser/parse "\"14 name\"" :start :SQL_TOKEN)))
  (is (= [:SQL_TOKEN "\"" "Name \"\"Quotes\"\" Name" "\""]
         (parser/parse "\"Name \"\"Quotes\"\" Name\"" :start :SQL_TOKEN))))

(deftest parse-template-expr
  (testing "template-vars"
    (is (= [:TEMPLATE_EXPR [:TEMPLATE_VAR "var-name"]]
           (parser/parse "{{ var-name }}" :start :TEMPLATE_EXPR)))))

(deftest parse-named-param
  (is (= [:NAMED_PARAM "param-name"]
         (parser/parse ":param-name" :start :NAMED_PARAM))))
