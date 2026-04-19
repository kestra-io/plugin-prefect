# Kestra Prefect Plugin

## What

- Provides plugin components under `io.kestra.plugin.prefect`.
- Includes classes such as `CreateFlowRun`, `PrefectConnection`, `PrefectResponse`.

## Why

- What user problem does this solve? Teams need to trigger Prefect deployments and monitor flow runs from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Prefect steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Prefect.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `prefect`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.prefect.CreateFlowRun`

### Project Structure

```
plugin-prefect/
├── src/main/java/io/kestra/plugin/prefect/
├── src/test/java/io/kestra/plugin/prefect/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
