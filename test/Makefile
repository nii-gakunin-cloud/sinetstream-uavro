UPYTHON = $(shell . ./subr.sh; find_micropython)
REPO_TOP = $(shell realpath ..)
MICROPYPATH = $(shell $(UPYTHON) -c 'import sys; print(":".join(sys.path), end="")'):$(REPO_TOP)/src:$(REPO_TOP)/plugins/broker/mqtt/src

unittest::
	for X in *.py; do echo $$X; MICROPYPATH=$(MICROPYPATH) $(UPYTHON) $$X; done
py::
	MICROPYPATH=$(MICROPYPATH) $(UPYTHON)

run::
	docker compose up --force-recreate --abort-on-container-exit --no-color

#docker_image=sinetstream-upython-test
#build::
#	docker build --file ./Dockerfile --tag $(docker_image) docker
#
#workdir=/sinetstream-upython
#run::
#	docker run -it --rm --volume $(REPO_TOP):$(workdir) --workdir=$(workdir) $(docker_image) /bin/sh -c 'cd test && ./run-test.sh'
#bash sh::
#	docker run -it --rm --volume $(REPO_TOP):$(workdir) --workdir=$(workdir) $(docker_image) /bin/sh -c 'cd test && ./install-lib.sh && $@'
