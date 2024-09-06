# HLS LPDAAC Reconciliation

HLS LPDAAC Reconciliation is an AWS CloudFormation stack deployed via the AWS
CDK to the HLS account in MCP.  As such, deployment occurs only via GitHub
Workflows.

For local development work, you must have `tox` installed (ideally version 4+,
but at a minimum, 3.18).

If you want to be able to resolve dependency package references in a REPL or an
IDE, run the following command to create a development virtual environment in
the directory `venv`:

```plain
make venv
```

You may then select this virtual environment within your IDE in order to resolve
references, or run the following to resolve references within a REPL:

```plain
source venv/bin/activate
```

To run unit tests, run the following, which will create a separate virtual
environment in the directory `.venv` (notice the leading dot [`.`]), so it will
not affect the virtual environment for your IDE:

```plain
make unit-tests
```
