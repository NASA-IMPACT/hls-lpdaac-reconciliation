# HLS LPDAAC Reconciliation

HLS LPDAAC Reconciliation is an AWS CloudFormation stack deployed via the AWS
CDK to the HLS account in MCP.  As such, deployment occurs only via GitHub
Workflows.

For local development work, you must have `tox` installed (ideally version 4+,
but at a minimum, 3.18).

To create a development virtual environment in the directory `venv`, run the
following:

```plain
make venv
```

You may select this virtual environment within your IDE in order to resolve
references.

To run unit tests, run the following, which will create a separate virtual
environment in the directory `.venv`, so it will not affect the virtual
environment for your IDE:

```plain
make unit-tests
```
