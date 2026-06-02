# How to use the Prefect plugin

Trigger Prefect flow runs from Kestra flows and optionally wait for completion.

## Authentication

Set `apiKey` to your Prefect API key. For Prefect Cloud, also set `accountId` and `workspaceId` (both UUIDs from your Cloud workspace URL). For self-hosted Prefect, set `apiUrl` to your server's API endpoint (e.g. `http://127.0.0.1:4200/api`) and omit `accountId`/`workspaceId`. Store `apiKey` in a [secret](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`CreateFlowRun` triggers a flow run from a deployment — set `deploymentId` to the deployment UUID and optionally pass `parameters` as a map. By default the task waits for the run to reach a terminal state (`wait: true`); set `wait: false` to return immediately after creation. Control polling with `pollFrequency` (default 5 seconds). The output includes `flowRunId`, `state`, and `flowRunUrl`.
