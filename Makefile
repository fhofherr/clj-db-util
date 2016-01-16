.PHONY: vgup pgtest h2test integration-test

vgup:
	vagrant up

pgtest: vgup
	vagrant ssh -c "cd /vagrant && lein with-profile +postgres test :integration"

h2test:
	lein with-profile +h2 test :integration

integration-test: pgtest h2test
