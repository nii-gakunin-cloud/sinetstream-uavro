PYTHON = python3

lint:: pylint flake8

pylint:: linter/bin/pylint
	-linter/bin/pylint src/uavro/*.py
flake8:: linter/bin/flake8
	-linter/bin/flake8 src/uavro/*.py

linter/bin/pylint: linter
	linter/bin/pip install pylint
linter/bin/flake8: linter
	linter/bin/pip install flake8

linter:
	$(PYTHON) -m venv linter

distclean::
	-rm -rf linter
