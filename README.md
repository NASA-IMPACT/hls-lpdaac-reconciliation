# HLS LPDAAC Reconciliation

HLS LPDAAC Reconciliation is an AWS CloudFormation stack deployed via the AWS
CDK to the HLS account in MCP.  As such, deployment occurs only via GitHub
Workflows.

For local development work, you must have `uv` installed.

If you want to be able to resolve dependency package references in a REPL or an
IDE, run the following command to create a development virtual environment in
the directory `.venv`:

```plain
uv sync
```

Please also install the configured pre-commit hooks:

```plain
uv run pre-commit install --install-hooks
```

You may then select this virtual environment within your IDE in order to resolve
references, or run the following to resolve references within a REPL:

```plain
source .venv/bin/activate
```

To run unit tests, run the following:

```plain
make unit-tests
```

To run integration tests, you must have active AWS credentials.  To obtain
AWS short-term access keys:

- Connect to the NASA VPN.
- Login to [NASA's MCP portal](https://login.mcp.nasa.gov/login).
- From the IMPACT-HLS project, generate short-term access keys (either set
  environment variables or add an AWS profile, whichever you prefer).

Set the following environment variables:

- `HLS_STACK_NAME`: A unique value, such as a username or unique nickname,
  which will be used as a prefix for your integration tests stack.
- `HLS_LPDAAC_NOTIFICATION_EMAIL_ADDRESS`: Your email address, so you can
  receive email notifications during the integration test for handling
  reconciliation report responses.

  **NOTE:** Each time you deploy your integration test stack, either initially
  or after recreating it after destroying it, you will receive an email
  requesting that you confirm subscription to the response topic that is part of
  the integration tests resources stack.  You must confirm the subscription in
  order to receive the notification email that will be generated during test
  execution.  When you destroy your integration test stack, the subscription
  will be removed.

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

**NOTE:** There is one manual check that you must perform.  In addition to
confirming the email subscription noted above (as part of the deployment of your
integration tests resources), once you run the integration tests, you must
confirm that you receive an email notification.  If you do _not_ receive an
email (check your spam folder), this indicates a test failure.

Redeploy your integration tests stack as many times as necessary while
developing and running integration tests.

When you're finished, cleanup your integration tests stack with the following
command:

```plain
make destroy-it
```

Finally, when opening or updating (synchronizing) a Pull Request, GitHub will
trigger the same deploy/run/destroy cycle for integration tests, and will
automatically use your public GitHub email address as the value of the
`HLS_LPDAAC_NOTIFICATION_EMAIL_ADDRESS` environment variable, so there is no
need to set this variable in the GitHub repository's `dev` environment.  It is
set only in the `prod` environment.
