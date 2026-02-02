# Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

### TEMPLATED MANAGED FILE, DO NOT EDIT
## If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools

GRAFONNET_VERSION ?= v11.0.0
DASHBOARD_SOURCE ?= dashboard.jsonnet
DASHBOARD_GENERATED ?= dashboard.json

###############################################################################
## Documented Targets #########################################################
###############################################################################

.PHONY: clean
clean: ## clean environment and settings
	rm -rf vendor generated jsonnetfile.lock.json

.PHONY: install
install: ## install `grafonnet` library dependency via `jsonnet-bundler`
	jb install github.com/grafana/grafonnet/gen/grafonnet-${GRAFONNET_VERSION}@main

.PHONY: generate
generate: ## generate Grafana dashboard from jsonnet source
	jsonnet -J vendor -o ${DASHBOARD_GENERATED} ${DASHBOARD_SOURCE} --ext-str grafonnet_version=${GRAFONNET_VERSION}

.PHONY: check
check: ## draw attention to red flags in jsonnet source (and its includes)
	jsonnet-lint -J vendor ${DASHBOARD_SOURCE}

.PHONY: reformat
reformat: ## automatically fix stylistic problems in jsonnet files
	@find -L . \
		-name 'vendor' -prune \
		-o -name '*.libsonnet' -print \
		-o -name '*.jsonnet' -print \
		| xargs -n 1 -- jsonnetfmt --no-use-implicit-plus -i
