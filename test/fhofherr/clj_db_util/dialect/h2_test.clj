(ns fhofherr.clj-db-util.dialect.h2-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.dialect :as d]
            [fhofherr.clj-db-util.dialect.h2 :as h2]))

(deftest parse-boolean
  (is (= [:BOOLEAN [:TRUE]] (h2/h2-parser "TRUE" :start :BOOLEAN)))
  (is (= [:BOOLEAN [:TRUE]] (h2/h2-parser "true" :start :BOOLEAN)))
  (is (= [:BOOLEAN [:FALSE]] (h2/h2-parser "FALSE" :start :BOOLEAN)))
  (is (= [:BOOLEAN [:FALSE]] (h2/h2-parser "false" :start :BOOLEAN))))

(deftest parse-null
  (is (= [:NULL] (h2/h2-parser "null" :start :NULL)))
  (is (= [:NULL] (h2/h2-parser "NULL" :start :NULL))))

(deftest parse-string
  (is (= [:STRING "'" "'"] (h2/h2-parser "''" :start :STRING)))
  (is (= [:STRING "'" "something" "'"] (h2/h2-parser "'something'" :start :STRING)))
  (is (= [:STRING "'" "John''s car." "'"] (h2/h2-parser "'John''s car.'" :start :STRING))))

(deftest parse-dollar-quoted-string
  (is (= [:DOLLAR-QUOTED-STRING]
         (h2/h2-parser "$$$$" :start :DOLLAR-QUOTED-STRING)))
  (is (= [:DOLLAR-QUOTED-STRING "$"]
         (h2/h2-parser "$$$$$" :start :DOLLAR-QUOTED-STRING)))
  (is (= [:DOLLAR-QUOTED-STRING "Hello World"]
         (h2/h2-parser "$$Hello World$$" :start :DOLLAR-QUOTED-STRING)))
  (is (= [:DOLLAR-QUOTED-STRING "This costs 15$"]
         (h2/h2-parser "$$This costs 15$$$" :start :DOLLAR-QUOTED-STRING)))
  (is (= [:DOLLAR-QUOTED-STRING "$15 is too mutch"]
         (h2/h2-parser "$$$15 is too mutch$$" :start :DOLLAR-QUOTED-STRING)))
  (is (= [:DOLLAR-QUOTED-STRING "15$ is a fair price"]
         (h2/h2-parser "$$15$ is a fair price$$" :start :DOLLAR-QUOTED-STRING))))

(deftest parse-integral-number
  (is (= [:INTEGRAL-NUMBER "12345"]
         (h2/h2-parser "12345" :start :INTEGRAL-NUMBER)))
  (is (= [:INTEGRAL-NUMBER "+" "12345"]
         (h2/h2-parser "+12345" :start :INTEGRAL-NUMBER)))
  (is (= [:INTEGRAL-NUMBER "-" "12345"]
         (h2/h2-parser "-12345" :start :INTEGRAL-NUMBER))))

(deftest parse-decimal-number
  (is (= [:DECIMAL-NUMBER  "123" "." "45"]
         (h2/h2-parser "123.45" :start :DECIMAL-NUMBER)))
  (is (= [:DECIMAL-NUMBER "." "45"]
         (h2/h2-parser ".45" :start :DECIMAL-NUMBER)))
  (is (= [:DECIMAL-NUMBER "1""." "23" "E" "45"]
         (h2/h2-parser "1.23E45" :start :DECIMAL-NUMBER)))
  (is (= [:DECIMAL-NUMBER "1" "." "23" "E" "+" "45"]
         (h2/h2-parser "1.23E+45" :start :DECIMAL-NUMBER)))
  (is (= [:DECIMAL-NUMBER "1" "." "23" "E" "-" "45"]
         (h2/h2-parser "1.23E-45" :start :DECIMAL-NUMBER)))
  (is (= [:DECIMAL-NUMBER "." "23" "E" "-" "45"]
         (h2/h2-parser ".23E-45" :start :DECIMAL-NUMBER))))

(deftest parse-hex-number
  (is (= [:HEX-NUMBER [:HEX "4F"]]
         (h2/h2-parser "0x4F" :start :HEX-NUMBER)))
  (is (= [:HEX-NUMBER [:HEX "4f"]]
         (h2/h2-parser "0x4f" :start :HEX-NUMBER)))
  (is (= [:HEX-NUMBER "+" [:HEX "4F"]]
         (h2/h2-parser "+0x4F" :start :HEX-NUMBER)))
  (is (= [:HEX-NUMBER "-" [:HEX "4F"]]
         (h2/h2-parser "-0x4F" :start :HEX-NUMBER))))

(deftest parse-date
  (is (= [:DATE "2014-01-01"] (h2/h2-parser "DATE '2014-01-01'" :start :DATE)))
  (is (= [:DATE "2014-01-01"] (h2/h2-parser "date '2014-01-01'" :start :DATE))))

(deftest parse-time
  (is (= [:TIME "23:59:59"] (h2/h2-parser "TIME '23:59:59'" :start :TIME)))
  (is (= [:TIME "23:59:59"] (h2/h2-parser "time '23:59:59'" :start :TIME))))

(deftest parse-timestamp
  (is (= [:TIMESTAMP "2014-01-01 23:59:59"]
         (h2/h2-parser "TIMESTAMP '2014-01-01 23:59:59'" :start :TIMESTAMP)))
  (is (= [:TIMESTAMP "2014-01-01 23:59:59"]
         (h2/h2-parser "timestamp '2014-01-01 23:59:59'" :start :TIMESTAMP)))
  (is (= [:TIMESTAMP "2014-01-01 23:59:59.000000000"]
         (h2/h2-parser "timestamp '2014-01-01 23:59:59.000000000'"
                       :start :TIMESTAMP))))

(deftest parse-name
  (is (= ["_"] (h2/h2-parser "_" :start :NAME)))
  (is (= ["N"] (h2/h2-parser "N" :start :NAME)))
  (is (= ["n"] (h2/h2-parser "n" :start :NAME)))
  (is (= ["_NAME"] (h2/h2-parser "_NAME" :start :NAME)))
  (is (= ["NAME"] (h2/h2-parser "NAME" :start :NAME)))
  (is (= ["_NAME"] (h2/h2-parser "_NAME" :start :NAME)))
  (is (= ["name"] (h2/h2-parser "name" :start :NAME)))
  (is (= ["_name"] (h2/h2-parser "_name" :start :NAME)))
  (is (= ["\"" "NAME" "\""] (h2/h2-parser "\"NAME\"" :start :NAME)))
  (is (= ["\"" "name" "\""] (h2/h2-parser "\"name\"" :start :NAME)))
  (is (= ["\"" "14 name" "\""] (h2/h2-parser "\"14 name\"" :start :NAME)))
  (is (= ["\"" "Name \"\"Quotes\"\" Name" "\""]
         (h2/h2-parser "\"Name \"\"Quotes\"\" Name\"" :start :NAME))))

(deftest parse-array
  (is (= [:ARRAY] (h2/h2-parser "()" :start :ARRAY)))
  (is (= [:ARRAY [:INTEGRAL-NUMBER "1"]] (h2/h2-parser "(1,)" :start :ARRAY)))
  (is (= [:ARRAY [:INTEGRAL-NUMBER "1"] [:INTEGRAL-NUMBER "2"]]
         (h2/h2-parser "(1,2)" :start :ARRAY)))
  (is (= [:ARRAY [:INTEGRAL-NUMBER "1"] [:INTEGRAL-NUMBER "2"]]
         (h2/h2-parser "(1, 2)" :start :ARRAY)))
  (is (= [:ARRAY [:INTEGRAL-NUMBER "1"] [:INTEGRAL-NUMBER "2"]]
         (h2/h2-parser "(1, 2,)" :start :ARRAY))))

(deftest parse-template-var
  (is (= [:TEMPLATE-VAR "{{ " [:TEMPLATE-VAR-NAME "name"] " }}"]
         (h2/h2-parser "{{ name }}" :start :TEMPLATE-VAR)))
  (is (= [:TEMPLATE-VAR "${ " [:TEMPLATE-VAR-NAME "name"] " }"]
         (h2/h2-parser "${ name }" :start :TEMPLATE-VAR))))

(deftest parse-select
  (is (= [:SELECT-STMT
          [:SELECT]
          [:SELECT-EXPR "*"]
          [:FROM]
          [:TABLE-EXPR
           [:TABLE-NAME "dual"]]]
         (h2/h2-parser "select * from dual" :start :SELECT-STMT)))
  (is (= [:SELECT-STMT
          [:SELECT]
          [:SELECT-EXPR "*"]
          [:FROM]
          [:TABLE-EXPR
           [:TABLE-NAME "dual"]]
          [:WHERE]
          [:INTEGRAL-NUMBER "1"]
          [:EQ]
          [:INTEGRAL-NUMBER "1"]])
      (h2/h2-parser "select * from dual where 1=1" :start :SELECT-STMT))
  (is (= [:SELECT-STMT
          [:SELECT]
          [:SELECT-EXPR "*"]
          [:FROM]
          [:TABLE-EXPR
           [:TABLE-NAME "dual"]]
          [:WHERE]
          [:INTEGRAL-NUMBER "1"]
          [:EQ]
          [:INTEGRAL-NUMBER "1"]])
      (h2/h2-parser "select * from dual where 1 = 1" :start :SELECT-STMT))
  (is (= [:SELECT-STMT
          [:SELECT]
          [:SELECT-EXPR "*"]
          [:FROM]
          [:TABLE-EXPR
           [:TABLE-NAME "dual"]]
          [:WHERE]
          [:INTEGRAL-NUMBER "1"]
          [:EQ]
          [:NAMED-PARAM "number"]])
      (h2/h2-parser "select * from dual where 1 = :number" :start :SELECT-STMT)))

(defn- load-and-trim
  [dialect stmt-path]
  (-> (d/load-statement d/h2 stmt-path)
      (clojure.string/replace #"\s*;?\s*$" "")))

(deftest parse-and-generate-roundtrips

  (testing "schema names"
    (let [sql-str (load-and-trim d/h2 "simple-select-with-schema.sql")
          tree (d/parse d/h2 sql-str)]
      (is (= sql-str (d/ast-to-str d/h2 tree))))))
