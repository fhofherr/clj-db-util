(ns fhofherr.clj-db-util.test.core.named-params-test
  (:require [clojure.test :refer :all]
            [fhofherr.clj-db-util.core.named-params :as named-params]))

(deftest parse-sql-str

  (testing "return named parameters in the order of occurence"
    (is (= {:sql-str "?" :params  [:param1]}
           (named-params/parse-sql-str ":param1")))
    (is (= {:sql-str "? ?" :params [:param1 :param2]}
           (named-params/parse-sql-str ":param1 :param2")))
    (is (= {:sql-str "?,?" :params [:param1 :param2]}
           (named-params/parse-sql-str ":param1,:param2"))))

  (testing "parameters are treated independent of each other"
    (is (= {:sql-str "? ?" :params [:param :param]}
           (named-params/parse-sql-str ":param :param"))))

  (testing "consecutive parameters are treated as one"
    (is (= {:sql-str "?" :params [:param1:param2]}
           (named-params/parse-sql-str ":param1:param2"))))

  (testing "parameters in quotes are ignored"
    (is (= {:sql-str "? ':ignored'" :params [:param]}
           (named-params/parse-sql-str ":param ':ignored'")))
    (is (= {:sql-str "? ''':ignored'''" :params [:param]}
           (named-params/parse-sql-str ":param ''':ignored'''")))
    (is (= {:sql-str "\":ignored\" ?" :params [:param]}
           (named-params/parse-sql-str "\":ignored\" :param")))
    (is (= {:sql-str "\"\"\":ignored\"\"\" ?" :params [:param]}
           (named-params/parse-sql-str "\"\"\":ignored\"\"\" :param"))))

  (testing "all other text is returned as-is"
    (is (= {:sql-str "SELECT * FROM t_key_value_pairs" :params []}
           (named-params/parse-sql-str "SELECT * FROM t_key_value_pairs")))))

(defn- input-consumed?
  [{:keys [input]}]
  (nil? input))

(defn- final-parse?
  [parse]
  (if-not (and (input-consumed? parse)
               (nil? (:accept-fn parse)))
    (do
      (printf "Input consumed? %s\n" (input-consumed? parse))
      (printf "accept-fn == nil? %s\n" (nil? (:accept-fn parse)))
      false)
    true))

(defn- parse-result=
  [expected {:keys [parse-result]}]
  (= expected parse-result))

(deftest accept-any-token
  (testing "accept anything except whitespace"

    (let [next-parse (-> "not-white-space"
                         (named-params/init-parse)
                         (named-params/accept-any-token))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:any-token "not-white-space"]] next-parse)))))

(deftest accept-whitespace
  (testing "accept only whitespace"

    (let [next-parse (-> " \r\n\t,"
                         (named-params/init-parse)
                         (named-params/accept-whitespace))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:whitespace " \r\n\t,"]] next-parse)))))

(deftest accept-named-parameter
  (testing "accept a colon followed by at least one non-whitespace character"
    (let [next-parse (-> ":named-param"
                         (named-params/init-parse)
                         (named-params/accept-named-parameter))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:named-param "named-param"]] next-parse)))))

(deftest accept-quoted-string
  (testing "accept strings in single quotes"
    (let [next-parse (-> "''"
                         (named-params/init-parse)
                         (named-params/accept-quoted-string))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:quoted-string "''"]] next-parse)))

    (let [next-parse (-> "':something'"
                         (named-params/init-parse)
                         (named-params/accept-quoted-string))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:quoted-string "':something'"]] next-parse))))

  (testing "accept strings in double quotes"
    (let [next-parse (-> "\"\""
                         (named-params/init-parse)
                         (named-params/accept-quoted-string))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:quoted-string "\"\""]] next-parse)))

    (let [next-parse (-> "\":something\""
                         (named-params/init-parse)
                         (named-params/accept-quoted-string))]
      (is (final-parse? next-parse))
      (is (parse-result= [[:quoted-string "\":something\""]] next-parse)))))

(deftest apply-accept-fn

  (testing "return the parse result if the input is fully consumed"
    (let [result (-> "some-token"
                     (named-params/init-parse)
                     (assoc :accept-fn named-params/accept-any-token)
                     (named-params/apply-accept-fn))]
      (is (= [[:any-token "some-token"]] result))))

  (testing "return a function if there is some input left"
    (let [intermediate-result (-> "some-token\t"
                                  (named-params/init-parse)
                                  (assoc :accept-fn named-params/accept-any-token)
                                  (named-params/apply-accept-fn))
          final-result (intermediate-result)]
      (is (function? intermediate-result))
      (is (= [[:any-token "some-token"] [:whitespace "\t"]] final-result)))))

(deftest dispatch
  (testing "dispatch to accept-named-parameter"
    (is (= named-params/accept-named-parameter (named-params/dispatch \:))))

  (testing "dispatch to accept-whitespace"
    (is (= named-params/accept-whitespace (named-params/dispatch \space)))
    (is (= named-params/accept-whitespace (named-params/dispatch \newline)))
    (is (= named-params/accept-whitespace (named-params/dispatch \tab)))
    (is (= named-params/accept-whitespace (named-params/dispatch \return)))
    (is (= named-params/accept-whitespace (named-params/dispatch \,))))

  (testing "dispatch to accept-quoted-string"
    (is (= named-params/accept-quoted-string (named-params/dispatch \')))
    (is (= named-params/accept-quoted-string (named-params/dispatch \"))))

  (testing "dispatch to accept-any-token"
    (is (= named-params/accept-any-token (named-params/dispatch \x)))))