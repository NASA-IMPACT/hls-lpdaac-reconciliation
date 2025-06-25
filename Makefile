SHELL=/usr/bin/env bash
UV=uv run $(UV_OPTS)
UV_OPTS?=--no-progress

.PHONY: help integration-tests unit-tests
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

.venv/bin/npm:
	$(UV) nodeenv --node lts --python-virtualenv

.venv/bin/cdk: .venv/bin/npm
	$(UV) npm install --location global "aws-cdk@latest"

## unit-tests: Run unit tests
unit-tests:
	$(UV) pytest

## integration-tests: Run integration tests (must run deploy-it first)
integration-tests:
	$(UV) pytest tests/integration

## synth: Run CDK synth
synth: .venv/bin/cdk
	$(UV) cdk synth

## diff: Run CDK diff
diff: .venv/bin/cdk
	$(UV) cdk diff

## deploy: Run CDK deploy
deploy: .venv/bin/cdk
	$(UV) cdk deploy --progress events --require-approval never

## destroy: Run CDK destroy
destroy: .venv/bin/cdk
	$(UV) cdk destroy --progress events --force

## synth-it: Run CDK synth for integration stack
synth-it: .venv/bin/cdk
	$(UV) cdk synth --app "python cdk/app_it.py" --all

## diff-it: Run CDK diff for integration stack
diff-it: .venv/bin/cdk
	$(UV) cdk diff --app "python cdk/app_it.py" --all

## deploy-it: Run CDK deploy for integration stack
deploy-it: .venv/bin/cdk
	$(UV) cdk deploy --app "python cdk/app_it.py" --all --progress events --require-approval never --outputs-file cdk.out/outputs.json

## destroy-it: Run CDK destroy for integration stack
destroy-it: .venv/bin/cdk
	$(UV) cdk destroy --app "python cdk/app_it.py" --all --progress events --force
