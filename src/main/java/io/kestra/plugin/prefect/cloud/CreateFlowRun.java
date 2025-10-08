package io.kestra.plugin.prefect.cloud;

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
    title = "Create a flow run in Prefect Cloud",
    description = "Trigger a deployment and optionally wait for the flow run to complete."
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
                    type: io.kestra.plugin.prefect.cloud.CreateFlowRun
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
                    type: io.kestra.plugin.prefect.cloud.CreateFlowRun
                    accountId: "{{ secret('PREFECT_ACCOUNT_ID') }}"
                    workspaceId: "{{ secret('PREFECT_WORKSPACE_ID') }}"
                    deploymentId: "{{ secret('PREFECT_DEPLOYMENT_ID') }}"
                    apiKey: "{{ secret('PREFECT_API_KEY') }}"
                    wait: false
                """
        )
    }
)
public class CreateFlowRun extends Task implements RunnableTask<CreateFlowRun.Output> {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();
    
    @Schema(
        title = "Prefect Cloud API key",
        description = "API key for authenticating with Prefect Cloud. You can create an API key from your Prefect Cloud account settings."
    )
    @NotNull
    private Property<String> apiKey;

    @Schema(
        title = "Prefect Cloud account ID",
        description = "The account ID (UUID) in Prefect Cloud."
    )
    @NotNull
    private Property<String> accountId;

    @Schema(
        title = "Prefect Cloud workspace ID",
        description = "The workspace ID (UUID) in Prefect Cloud."
    )
    @NotNull
    private Property<String> workspaceId;

    @Schema(
        title = "Deployment ID",
        description = "The deployment ID (UUID) to create a flow run from."
    )
    @NotNull
    private Property<String> deploymentId;

    @Schema(
        title = "Wait for flow run completion",
        description = "Whether to wait for the flow run to complete before continuing. If true, the task will poll the flow run status until it reaches a terminal state (COMPLETED, FAILED, CANCELLED, etc.)."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.of(true);

    @Schema(
        title = "Poll frequency",
        description = "How often to poll the flow run status when wait is true."
    )
    @Builder.Default
    private Duration pollFrequency = Duration.ofSeconds(5);

    @Schema(
        title = "Flow run parameters",
        description = "Optional parameters to pass to the flow run."
    )
    private Map<String, Object> parameters;

    @Schema(
        title = "Base URL",
        description = "Base URL for Prefect Cloud API. Defaults to https://api.prefect.cloud/api"
    )
    private Property<String> baseUrl;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Build connection
        PrefectConnection connection = PrefectConnection.builder()
            .apiKey(this.apiKey)
            .accountId(this.accountId)
            .workspaceId(this.workspaceId)
            .baseUrl(this.baseUrl != null ? this.baseUrl : Property.of("https://api.prefect.cloud/api"))
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

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
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

            HttpResponse<String> statusResponse = httpClient.send(statusRequest, HttpResponse.BodyHandlers.ofString());
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
        String rAccountId = runContext.render(connection.getAccountId()).as(String.class).orElseThrow();
        String rWorkspaceId = runContext.render(connection.getWorkspaceId()).as(String.class).orElseThrow();
        
        return String.format("https://app.prefect.cloud/account/%s/workspace/%s/flow-runs/flow-run/%s",
            rAccountId, rWorkspaceId, flowRunId);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Flow run ID",
            description = "The ID of the created flow run"
        )
        private final String flowRunId;

        @Schema(
            title = "Flow run state",
            description = "The final state of the flow run (if wait is true) or the initial state"
        )
        private final String state;

        @Schema(
            title = "Flow run URL",
            description = "The URL to view the flow run in Prefect Cloud"
        )
        private final String flowRunUrl;
    }
}

