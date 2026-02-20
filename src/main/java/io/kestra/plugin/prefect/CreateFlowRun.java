package io.kestra.plugin.prefect;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a Prefect deployment run",
    description = "Creates a flow run from a Prefect deployment and can wait until it reaches a terminal state. Works with Prefect Cloud (account and workspace required) and self-hosted APIs; waits poll every 5 seconds by default and fails on FAILED/CRASHED/CANCELLED when waiting."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a Prefect Cloud deployment and wait for completion",
            full = true,
            code = """
                id: prefect_trigger
                namespace: company.team
                
                tasks:
                  - id: trigger_prefect_run
                    type: io.kestra.plugin.prefect.CreateFlowRun
                    apiUrl: "https://api.prefect.cloud/api"
                    accountId: "{{ secret('PREFECT_ACCOUNT_ID') }}"
                    workspaceId: "{{ secret('PREFECT_WORKSPACE_ID') }}"
                    deploymentId: "{{ secret('PREFECT_DEPLOYMENT_ID') }}"
                    apiKey: "{{ secret('PREFECT_API_KEY') }}"
                    wait: true
                    pollFrequency: PT10S
                """
        ),
        @Example(
            title = "Trigger a Prefect Cloud deployment without waiting",
            code = """
                id: prefect_trigger
                namespace: company.team
                
                tasks:
                  - id: trigger_prefect_run
                    type: io.kestra.plugin.prefect.CreateFlowRun
                    apiUrl: "https://api.prefect.cloud/api"
                    accountId: "{{ secret('PREFECT_ACCOUNT_ID') }}"
                    workspaceId: "{{ secret('PREFECT_WORKSPACE_ID') }}"
                    deploymentId: "{{ secret('PREFECT_DEPLOYMENT_ID') }}"
                    apiKey: "{{ secret('PREFECT_API_KEY') }}"
                    wait: false
                """
        ),
        @Example(
            title = "Trigger a self-hosted Prefect deployment (without authentication)",
            full = true,
            code = """
                id: prefect_self_hosted
                namespace: company.team
                
                tasks:
                  - id: trigger_prefect_run
                    type: io.kestra.plugin.prefect.CreateFlowRun
                    apiUrl: "http://host.docker.internal:4200/api"
                    deploymentId: "{{ secret('PREFECT_DEPLOYMENT_ID') }}"
                    wait: true
                    pollFrequency: PT10S
                """
        ),
        @Example(
            title = "Trigger a self-hosted Prefect deployment with Basic authentication",
            code = """
                id: prefect_self_hosted_auth
                namespace: company.team
                
                tasks:
                  - id: trigger_prefect_run
                    type: io.kestra.plugin.prefect.CreateFlowRun
                    apiUrl: "http://host.docker.internal:4200/api"
                    deploymentId: "{{ secret('PREFECT_DEPLOYMENT_ID') }}"
                    apiKey: "{{ secret('PREFECT_BASIC_AUTH') }}"  # base64-encoded "admin:pass"
                    wait: true
                    pollFrequency: PT10S
                """
        ),
        @Example(
            title = "Pass parameters to the flow run",
            code = """
                id: prefect_with_params
                namespace: company.team
                
                tasks:
                  - id: trigger_prefect_run
                    type: io.kestra.plugin.prefect.CreateFlowRun
                    apiUrl: "https://api.prefect.cloud/api"
                    accountId: "{{ secret('PREFECT_ACCOUNT_ID') }}"
                    workspaceId: "{{ secret('PREFECT_WORKSPACE_ID') }}"
                    deploymentId: "{{ secret('PREFECT_DEPLOYMENT_ID') }}"
                    apiKey: "{{ secret('PREFECT_API_KEY') }}"
                    wait: true
                    parameters:
                      run_date: "{{ now() }}"
                      retries: 2
                      region: "us-east-1"
                """
        )
    }
)
public class CreateFlowRun extends Task implements RunnableTask<CreateFlowRun.Output> {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();
    
    @Schema(
        title = "Prefect API endpoint",
        description = "Base Prefect API URL. Defaults to `https://api.prefect.cloud/api`; for self-hosted instances provide your `/api` endpoint such as `http://127.0.0.1:4200/api`."
    )
    @Builder.Default
    private Property<String> apiUrl = Property.of("https://api.prefect.cloud/api");

    @Schema(
        title = "API credentials",
        description = """
            Authentication sent in the Authorization header. Prefect Cloud expects an API key (sent as Bearer); self-hosted can supply a base64 Basic token like "YWRtaW46cGFzcw==" or the full "Basic ..." header; leave empty for unauthenticated servers.
            """
    )
    private Property<String> apiKey;

    @Schema(
        title = "Prefect Cloud account ID",
        description = "Prefect Cloud account UUID required when calling the Cloud API."
    )
    private Property<String> accountId;

    @Schema(
        title = "Prefect Cloud workspace ID",
        description = "Prefect Cloud workspace UUID required when calling the Cloud API."
    )
    private Property<String> workspaceId;

    @Schema(
        title = "Deployment ID",
        description = "Deployment UUID used to create the flow run."
    )
    @NotNull
    private Property<String> deploymentId;

    @Schema(
        title = "Wait for flow run completion",
        description = "Whether to block until the flow run reaches a terminal state. Defaults to true; when true, FAILED/CRASHED/CANCELLED throw a task error."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.ofValue(true);

    @Schema(
        title = "Poll frequency",
        description = "Polling interval while waiting; defaults to PT5S and only used when wait is true."
    )
    @Builder.Default
    private Duration pollFrequency = Duration.ofSeconds(5);

    @Schema(
        title = "Flow run parameters",
        description = "Optional parameters passed to the flow run after rendering with the task context."
    )
    private Map<String, Object> parameters;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Build connection
        PrefectConnection connection = PrefectConnection.builder()
            .apiKey(this.apiKey)
            .accountId(this.accountId)
            .workspaceId(this.workspaceId)
            .apiUrl(this.apiUrl)
            .build();

        String rDeploymentId = runContext.render(deploymentId).as(String.class).orElseThrow();

        // Create flow run
        logger.info("Creating flow run for deployment: {}", rDeploymentId);
        
        Map<String, Object> requestBody = new HashMap<>();
        if (parameters != null && !parameters.isEmpty()) {
            requestBody.put("parameters", runContext.render(parameters));
        }

        HttpClient httpClient = PrefectConnection.httpClient();
        HttpRequest request = connection.request(runContext, "/deployments/" + rDeploymentId + "/create_flow_run")
            .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(requestBody)))
            .build();

        logger.debug("Sending request to: {}", request.uri());
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new Exception("Failed to connect to Prefect API at " + request.uri() + 
                              ". Please verify the Prefect server is running and accessible. Error: " + e.getMessage(), e);
        }
        Map<String, Object> flowRunResponse = PrefectResponse.parseResponseAsMap(response);

        String flowRunId = (String) flowRunResponse.get("id");
        logger.info("Created flow run with ID: {}", flowRunId);

        // Wait for completion if requested
        Boolean shouldWait = runContext.render(wait).as(Boolean.class).orElse(true);
        String finalState = null;
        
        if (shouldWait) {
            logger.info("Waiting for flow run to complete (polling every {})", pollFrequency);
            finalState = waitForCompletion(runContext, connection, httpClient, flowRunId);
            logger.info("Flow run completed with state: {}", finalState);
        }

        return Output.builder()
            .flowRunId(flowRunId)
            .state(finalState != null ? finalState : (String) ((Map<String, Object>) flowRunResponse.get("state")).get("type"))
            .flowRunUrl(getFlowRunUrl(runContext, connection, flowRunId))
            .build();
    }

    private String waitForCompletion(RunContext runContext, PrefectConnection connection, HttpClient httpClient, String flowRunId) throws Exception {
        while (true) {
            HttpRequest statusRequest = connection.request(runContext, "/flow_runs/" + flowRunId)
                .GET()
                .build();

            HttpResponse<String> statusResponse;
            try {
                statusResponse = httpClient.send(statusRequest, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new Exception("Failed to poll flow run status from Prefect API at " + statusRequest.uri() + 
                                  ". Please verify the Prefect server is running and accessible. Error: " + e.getMessage(), e);
            }
            Map<String, Object> flowRunData = PrefectResponse.parseResponseAsMap(statusResponse);

            Map<String, Object> state = (Map<String, Object>) flowRunData.get("state");
            String stateType = (String) state.get("type");

            // Terminal states in Prefect
            if (isTerminalState(stateType)) {
                if ("FAILED".equals(stateType) || "CRASHED".equals(stateType) || "CANCELLED".equals(stateType)) {
                    String stateName = (String) state.get("name");
                    String message = (String) state.get("message");
                    throw new Exception("Flow run ended in state: " + stateType + 
                                      (stateName != null ? " (" + stateName + ")" : "") +
                                      (message != null ? " - " + message : ""));
                }
                return stateType;
            }

            Thread.sleep(pollFrequency.toMillis());
        }
    }

    private boolean isTerminalState(String stateType) {
        return stateType.equals("COMPLETED") ||
               stateType.equals("FAILED") ||
               stateType.equals("CRASHED") ||
               stateType.equals("CANCELLED") ||
               stateType.equals("CANCELLING");
    }

    private String getFlowRunUrl(RunContext runContext, PrefectConnection connection, String flowRunId) throws Exception {
        if (connection.isCloud()) {
            // Prefect Cloud URL format
            String rAccountId = runContext.render(connection.getAccountId()).as(String.class).orElseThrow();
            String rWorkspaceId = runContext.render(connection.getWorkspaceId()).as(String.class).orElseThrow();
            
            return String.format("https://app.prefect.cloud/account/%s/workspace/%s/flow-runs/flow-run/%s",
                rAccountId, rWorkspaceId, flowRunId);
        } else {
            // Self-hosted Prefect URL format
            // Convert API URL to UI URL (typically same host but different port/path)
            String rApiUrl = runContext.render(connection.getApiUrl()).as(String.class).orElseThrow();
            String baseUrl = rApiUrl.replace("/api", "");
            
            return String.format("%s/flow-runs/flow-run/%s", baseUrl, flowRunId);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Flow run ID",
            description = "ID of the created flow run"
        )
        private final String flowRunId;

        @Schema(
            title = "Flow run state",
            description = "Terminal state when wait is true; initial state otherwise"
        )
        private final String state;

        @Schema(
            title = "Flow run URL",
            description = "URL to view the flow run in Prefect UI (Cloud or self-hosted)"
        )
        private final String flowRunUrl;
    }
}
