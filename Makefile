CDK_CONTEXT=--context permissions-boundary=mcp-tenantOperator
# CDK version must match the version specified in setup.py
CDK_VERSION=2.153.0
NODE_VERSION=20.16.0
SHELL=/usr/bin/env bash
TOX=tox $(TOX_OPTS)
TOX_OPTS?=-v

.PHONY: help install-cdk install-node integration-tests tox unit-tests
.DEFAULT_GOAL := help

help: Makefile
	@echo
	@echo "Usage: make [options] target ..."
	@echo
	@echo "Options:"
	@echo "  Run 'make -h' to list options."
	@echo
	@echo "Targets:"
	@sed -n 's/^##//p' $< | column -t -s ':' | sed -e 's/^/ /'
	@echo

tox:
	@if [[ -z $${TOX_ENV_DIR} ]]; then \
	    echo "ERROR: For tox.ini use only" >&2; \
	    exit 1; \
	fi

# NOTE: Intended only for use from tox.ini.
# Install Node.js within the tox virtualenv, if it's not installed or it's the wrong version.
install-node: tox
	@if [[ ! $$(type node 2>/dev/null) =~ $${VIRTUAL_ENV} || ! $$(node -v) =~ $(NODE_VERSION) ]]; then \
	    set -x; nodeenv --node $(NODE_VERSION) --python-virtualenv; \
	fi

# NOTE: Intended only for use from tox.ini
# Install the CDK CLI within the tox virtualenv, if it's not installed or it's the wrong version.
install-cdk: tox install-node
	@if [[ ! $$(type cdk 2>/dev/null) =~ $${VIRTUAL_ENV} || ! $$(cdk --version) =~ $(CDK_VERSION) ]]; then \
	    set -x; npm install --location global "aws-cdk@$(CDK_VERSION)"; \
	fi

## venv: Create Python virtual environment in directory `venv`
venv: setup.py
	$(TOX) devenv

## unit-tests: Run unit tests
unit-tests:
	$(TOX)

## integration-tests: Run integration tests (must run deploy-it first)
integration-tests:
	$(TOX) -e integration

## synth: Run CDK synth
synth:
	$(TOX) -e dev -- synth $(CDK_CONTEXT)

## diff: Run CDK diff
diff:
	$(TOX) -e dev -- diff $(CDK_CONTEXT)

## deploy: Run CDK deploy
deploy:
	$(TOX) -e dev -- deploy $(CDK_CONTEXT) --progress events --require-approval never

## destroy: Run CDK destroy
destroy:
	$(TOX) -e dev -- destroy $(CDK_CONTEXT) --progress events --force

## synth-it: Run CDK synth for integration stack
synth-it:
	$(TOX) -e dev -- synth $(CDK_CONTEXT) --app python cdk/app_it.py

## diff-it: Run CDK diff for integration stack
diff-it:
	$(TOX) -e dev -- diff $(CDK_CONTEXT) --app python cdk/app_it.py

## deploy-it: Run CDK deploy for integration stack
deploy-it:
	$(TOX) -e dev -- deploy $(CDK_CONTEXT) --app python cdk/app_it.py --progress events --require-approval never

## destroy-it: Run CDK destroy for integration stack
destroy-it:
	$(TOX) -e dev -- destroy $(CDK_CONTEXT) --app python cdk/app_it.py --progress events --force