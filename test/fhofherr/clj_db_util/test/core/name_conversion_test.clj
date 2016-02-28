(ns fhofherr.clj-db-util.test.core.name-conversion-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core :as db-util]
            [clojure.string :as string]))

(deftest convert-to-db-name
  (db-util/with-database
    [db (db-util/connect-to-db "jdbc:h2:mem:" "" "" {:to-db-name string/upper-case})]

    (testing "conversion of strings, keywords and symbols"
      (let [[res err] (db-util/with-db-transaction db (db-util/convert-to-db-name "hello-world"))]
        (is (nil? err))
        (is (= "HELLO-WORLD" res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-to-db-name :hello-world))]
        (is (nil? err))
        (is (= :HELLO-WORLD res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-to-db-name 'hello-world))]
        (is (nil? err))
        (is (= 'HELLO-WORLD res))))

    (testing "conversion of map keys"
      (let [[res err] (db-util/with-db-transaction db (db-util/convert-to-db-name {:key :value}))]
        (is (nil? err))
        (is (= {:KEY :value} res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-to-db-name {'key :value}))]
        (is (nil? err))
        (is (= {'KEY :value} res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-to-db-name {"key" :value}))]
        (is (nil? err))
        (is (= {"KEY" :value} res))))

    (testing "conversion of sequential collection entries"
      (let [[res err] (db-util/with-db-transaction db (db-util/convert-to-db-name ["hello" "world"]))]
        (is (nil? err))
        (is (= ["HELLO" "WORLD"] res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-to-db-name '("hello" "world")))]
        (is (nil? err))
        (is (= ["HELLO" "WORLD"] res))))))

(deftest convert-from-db-name
  (db-util/with-database
    [db (db-util/connect-to-db "jdbc:h2:mem:" "" "" {:from-db-name string/upper-case})]

    (testing "conversion of strings, keywords and symbols"
      (let [[res err] (db-util/with-db-transaction db (db-util/convert-from-db-name "hello-world"))]
        (is (nil? err))
        (is (= "HELLO-WORLD" res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-from-db-name :hello-world))]
        (is (nil? err))
        (is (= :HELLO-WORLD res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-from-db-name 'hello-world))]
        (is (nil? err))
        (is (= 'HELLO-WORLD res))))

    (testing "conversion of map keys"
      (let [[res err] (db-util/with-db-transaction db (db-util/convert-from-db-name {:key :value}))]
        (is (nil? err))
        (is (= {:KEY :value} res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-from-db-name {'key :value}))]
        (is (nil? err))
        (is (= {'KEY :value} res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-from-db-name {"key" :value}))]
        (is (nil? err))
        (is (= {"KEY" :value} res))))

    (testing "conversion of sequential collection entries"
      (let [[res err] (db-util/with-db-transaction db (db-util/convert-from-db-name ["hello" "world"]))]
        (is (nil? err))
        (is (= ["HELLO" "WORLD"] res)))

      (let [[res err] (db-util/with-db-transaction db (db-util/convert-from-db-name '("hello" "world")))]
        (is (nil? err))
        (is (= ["HELLO" "WORLD"] res))))))