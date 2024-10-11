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

To run integration tests, you must have active AWS credentials.  To obtain
AWS short-term access keys:

- Connect to the NASA VPN.
- Login to [NASA's MCP portal](https://login.mcp.nasa.gov/login).
- From the IMPACT-HLS project, generate short-term access keys (either set
  environment variables or add an AWS profile, whichever you prefer).
- Set the environment variable `HLS_STACK_NAME` to a unique value (such as your
  username), which will be used as a prefix for the integration tests stack.

At this point, you may disconnect from the NASA VPN and run the following to
deploy your own integration tests stack:

```plain
make deploy-it
```

To run integration tests, use the following command, which will use your
deployed integration tests stack:

```plain
make integration-tests
```

Redeploy your integration tests stack as many times as necessary while
developing and running integration tests.

When you're finished, cleanup your integration tests stack with the following
command:

```plain
make destroy-it
```
