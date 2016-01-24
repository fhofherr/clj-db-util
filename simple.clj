(defci)

(defjob unit-tests
  :test (execute "scripts/vagrant/unittest.sh")
  :triggers [{:type :timer
              :name "unit-test-trigger"
              :args [:every 5 :minutes]}])

(defjob h2-integration-tests
  :test (execute "scripts/vagrant/h2test.sh")
  :triggers [{:type :timer
              :name "h2-integration-test-trigger"
              :args [:every 30 :minutes]}])

(defjob postgres-integration-tests
  :test (execute "scripts/vagrant/pgtest.sh")
  :triggers [{:type :timer
              :name "postgres-integration-test-trigger"
              :args [:every 30 :minutes]}])
