WORKDIR=$(shell pwd)

SET_PATH=export PYTHONPATH=$(WORKDIR)
RUN=$(SET_PATH) && python
GREP ?= ALL
test-functional:
	$(RUN) ./mockthink/test/functional/__init__.py --run mockthink --grep $(GREP)

test-functional-rethink:
	$(RUN) ./mockthink/test/functional/__init__.py --run rethink --grep $(GREP)

test-integration:
	$(RUN) ./mockthink/test/integration/__init__.py --run mockthink --grep $(GREP)

test-integration-rethink:
	$(RUN) ./mockthink/test/integration/__init__.py --run rethink --grep $(GREP)

test-unit:
	$(SET_PATH) && nosetests ./mockthink/test/unit

test: test-unit test-integration test-functional


