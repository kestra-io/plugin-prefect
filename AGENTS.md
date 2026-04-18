# Kestra Prefect Plugin

## What

- Provides plugin components under `io.kestra.plugin.prefect`.
- Includes classes such as `CreateFlowRun`, `PrefectConnection`, `PrefectResponse`.

## Why

- This plugin integrates Kestra with Prefect.
- It provides tasks that trigger Prefect deployments and monitor flow runs.

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
