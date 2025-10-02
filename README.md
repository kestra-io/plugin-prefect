<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Event-Driven Declarative Orchestrator
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

<p align="center">
    <a href="https://twitter.com/kestra_io"><img height="25" src="https://kestra.io/twitter.svg" alt="twitter" /></a> &nbsp;
    <a href="https://www.linkedin.com/company/kestra/"><img height="25" src="https://kestra.io/linkedin.svg" alt="linkedin" /></a> &nbsp;
<a href="https://www.youtube.com/@kestra-io"><img height="25" src="https://kestra.io/youtube.svg" alt="youtube" /></a> &nbsp;
</p>

<br />
<p align="center">
    <a href="https://go.kestra.io/video/product-overview" target="_blank">
        <img src="https://kestra.io/startvideo.png" alt="Get started in 4 minutes with Kestra" width="640px" />
    </a>
</p>
<p align="center" style="color:grey;"><i>Get started with Kestra in 4 minutes.</i></p>


# Kestra Prefect Plugin

> Integrate Prefect Cloud workflows with Kestra

This plugin provides tasks to interact with Prefect Cloud, allowing you to trigger deployments and manage flow runs from your Kestra workflows.

## Features

- **CreateFlowRun**: Trigger a Prefect Cloud deployment and optionally wait for completion
- Supports all Prefect Cloud API authentication
- Configurable polling intervals for flow run status
- Comprehensive error handling

## Usage

### Trigger a Prefect Flow Run

```yaml
id: prefect_integration
namespace: company.team

tasks:
  - id: trigger_prefect_run
    type: io.kestra.plugin.prefect.cloud.CreateFlowRun
    accountId: "{{ secret('PREFECT_ACCOUNT_ID') }}"
    workspaceId: "{{ secret('PREFECT_WORKSPACE_ID') }}"
    deploymentId: "{{ secret('PREFECT_DEPLOYMENT_ID') }}"
    apiKey: "{{ secret('PREFECT_API_KEY') }}"
    wait: true
    pollFrequency: PT10S
```

### Configuration

The `CreateFlowRun` task supports the following properties:

- `accountId` (required): Your Prefect Cloud account ID (UUID)
- `workspaceId` (required): Your Prefect Cloud workspace ID (UUID)
- `deploymentId` (required): The deployment ID to trigger (UUID)
- `apiKey` (required): Your Prefect Cloud API key
- `wait` (optional, default: `true`): Wait for the flow run to complete
- `pollFrequency` (optional, default: `PT5S`): How often to poll for status when waiting
- `parameters` (optional): Parameters to pass to the flow run
- `baseUrl` (optional): Custom Prefect Cloud API URL (defaults to https://api.prefect.cloud/api)

### Getting Prefect Cloud Credentials

1. Log in to your Prefect Cloud account
2. Navigate to your account settings to find your Account ID and Workspace ID
3. Create an API key from the API Keys section
4. Find your deployment ID from the Deployments page

![Kestra orchestrator](https://kestra.io/video.gif)

## Running the project in local
### Prerequisites
- Java 21
- Docker

### Running tests
```
./gradlew check --parallel
```

### Development

`VSCode`:

Follow the README.md within the `.devcontainer` folder for a quick and easy way to get up and running with developing plugins if you are using VSCode.

`Other IDEs`:

```
./gradlew shadowJar && docker build -t kestra-custom . && docker run --rm -p 8080:8080 kestra-custom server local
```
> [!NOTE]
> You need to relaunch this whole command everytime you make a change to your plugin

go to http://localhost:8080, your plugin will be available to use

## Documentation
* Full documentation can be found under: [kestra.io/docs](https://kestra.io/docs)
* Documentation for developing a plugin is included in the [Plugin Developer Guide](https://kestra.io/docs/plugin-developer-guide/)


## License
Apache 2.0 © [Kestra Technologies](https://kestra.io)


## Stay up to date

We release new versions every month. Give the [main repository](https://github.com/kestra-io/kestra) a star to stay up to date with the latest releases and get notified about future updates.

![Star the repo](https://kestra.io/star.gif)
