.PHONY: vgup pgtest h2test unittest simple integration-test

vgup:
	vagrant up

pgtest: vgup
	vagrant ssh -c "/vagrant/scripts/vagrant/pgtest.sh"

h2test: vgup
	vagrant ssh -c "/vagrant/scripts/vagrant/h2test.sh"

unittest: vgup
	vagrant ssh -c "/vagrant/scripts/vagrant/unittest.sh"

simple: vgup
	vagrant ssh -c "java -jar /usr/local/lib/simple.jar /vagrant"

integration-test: pgtest h2test
