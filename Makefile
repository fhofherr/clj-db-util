.PHONY: vgup pgtest h2test integration-test

vgup:
	vagrant up

pgtest: vgup
	vagrant ssh -c "cd /vagrant && source /vagrant/scripts/vagrant/pg_env.sh && lein with-profile +postgres test :integration"

h2test: vgup
	vagrant ssh -c "cd /vagrant && source /vagrant/scripts/vagrant/h2_env.sh && lein with-profile +h2 test :integration"

integration-test: pgtest h2test
