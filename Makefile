.PHONY: vgup pgtest h2test unittest simple integration-test h2memtest

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

h2memtest:
	DB_USER="" DB_PASS="" DB_URL="jdbc:h2:mem:test;DB_CLOSE_DELAY=-1" lein with-profile +h2 test :integration

integration-test: pgtest h2test
