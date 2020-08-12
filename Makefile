IMAGE ?= "target-ndjson"
TAG ?= $(shell git log -1 --format=%h)

lint:
	pylint --rcfile .pylintrc *.py target_ndjson

build:
	docker build -f ops/Dockerfile -t $(IMAGE):$(TAG) .

test: build
	docker run --rm $(IMAGE):$(TAG) pytest

shell: build
	docker run -v $(PWD):/home/python -it $(IMAGE):$(TAG) bash

.PHONY: lint build test shell
