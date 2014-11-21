WORKDIR=$(shell pwd)

SET_PATH=export PYTHONPATH=$(WORKDIR)

test-functional:
	$(SET_PATH) && python ./mockthink/test/functional/__init__.py

test-integration:
	$(SET_PATH) && python ./mockthink/test/integration/__init__.py
