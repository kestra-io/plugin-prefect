package io.kestra.plugin.prefect;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a Prefect deployment run",
    description = "Creates a flow run from a Prefect deployment and can wait until it reaches a terminal state. Works with Prefect Cloud (account and workspace required) and self-hosted APIs; waits poll every 5 seconds by default and fails on FAILED/CRASHED/CANCELLED when waiting. Killing the Kestra task, or a worker shutdown, cancels the corresponding Prefect flow run when wait is true."
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
            full = true,
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
            full = true,
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
    @PluginProperty(group = "connection")
    private Property<String> apiUrl = Property.ofValue("https://api.prefect.cloud/api");

    @Schema(
        title = "API credentials",
        description = """
            Authentication sent in the Authorization header. Prefect Cloud expects an API key (sent as Bearer); self-hosted can supply a base64 Basic token like "YWRtaW46cGFzcw==" or the full "Basic ..." header; leave empty for unauthenticated servers.
            """
    )
    @PluginProperty(group = "connection", secret = true)
    private Property<String> apiKey;

    @Schema(
        title = "Prefect Cloud account ID",
        description = "Prefect Cloud account UUID required when calling the Cloud API."
    )
    @PluginProperty(group = "connection")
    private Property<String> accountId;

    @Schema(
        title = "Prefect Cloud workspace ID",
        description = "Prefect Cloud workspace UUID required when calling the Cloud API."
    )
    @PluginProperty(group = "advanced")
    private Property<String> workspaceId;

    @Schema(
        title = "Deployment ID",
        description = "Deployment UUID used to create the flow run."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> deploymentId;

    @Schema(
        title = "Wait for flow run completion",
        description = "Whether to block until the flow run reaches a terminal state. Defaults to true; when true, FAILED/CRASHED/CANCELLED throw a task error."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> wait = Property.ofValue(true);

    @Schema(
        title = "Poll frequency",
        description = "Polling interval while waiting; defaults to PT5S and only used when wait is true."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Duration pollFrequency = Duration.ofSeconds(5);

    @Schema(
        title = "Flow run parameters",
        description = "Optional parameters passed to the flow run after rendering with the task context."
    )
    @PluginProperty(group = "main")
    private Map<String, Object> parameters;

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<String> trackedFlowRunId = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<PrefectConnection> trackedConnection = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<HttpClient> trackedHttpClient = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<RunContext> trackedRunContext = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<String> lastKnownState = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final CountDownLatch cancelSignal = new CountDownLatch(1);

    /**
     * Killing the Kestra task, or a worker shutdown, must cancel the remote Prefect flow run instead of
     * orphaning it. {@code stop()} must be non-blocking, so the cancel HTTP call is best-effort.
     */
    @Override
    public void kill() {
        cancelFlowRun();
    }

    @Override
    public void stop() {
        cancelFlowRun();
    }

    private void cancelFlowRun() {
        if (!isCancelled.compareAndSet(false, true)) {
            return;
        }

        // Unblocks waitForCompletion() immediately instead of leaving it asleep for up to pollFrequency.
        cancelSignal.countDown();
        performCancel();
    }

    /**
     * Issues the actual Prefect cancel request. Split out from {@link #cancelFlowRun()} so the
     * kill-before-create race in {@code run()} can trigger it a second time once the flow run ID is
     * known, without re-entering the once-only {@code isCancelled} guard.
     *
     * <p>The HTTP call is dispatched asynchronously and never awaited on the calling thread: {@code kill()}
     * and {@code stop()} are invoked from the Kestra worker's lifecycle thread, which the {@code stop()}
     * contract requires to remain non-blocking. The 10s timeout still bounds the async request itself so it
     * doesn't hang forever, it just isn't awaited here.
     */
    private void performCancel() {
        String flowRunId = trackedFlowRunId.get();
        PrefectConnection connection = trackedConnection.get();
        HttpClient httpClient = trackedHttpClient.get();
        RunContext runContext = trackedRunContext.get();

        if (flowRunId == null || connection == null || httpClient == null || runContext == null) {
            return;
        }

        Logger logger = runContext.logger();

        // Mirrors `prefect flow-run cancel`: once infra is provisioned (RUNNING), ask the worker to tear
        // it down via CANCELLING; otherwise (SCHEDULED/PENDING/unknown) cancel directly.
        boolean infrastructureProvisioned = "RUNNING".equals(lastKnownState.get());
        String targetStateType = infrastructureProvisioned ? "CANCELLING" : "CANCELLED";
        String targetStateName = infrastructureProvisioned ? "Cancelling" : "Cancelled";

        try {
            Map<String, Object> state = new HashMap<>();
            state.put("type", targetStateType);
            state.put("name", targetStateName);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("state", state);
            requestBody.put("force", true);

            HttpRequest cancelRequest = connection.request(runContext, "/flow_runs/" + flowRunId + "/set_state")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(requestBody)))
                .build();

            httpClient.sendAsync(cancelRequest, HttpResponse.BodyHandlers.ofString())
                .whenComplete((cancelResponse, throwable) -> {
                    if (throwable != null) {
                        logger.warn("Failed to cancel Prefect flow run '{}', the flow run may still be running on Prefect", flowRunId, throwable);
                        return;
                    }

                    try {
                        Map<String, Object> cancelResponseBody = PrefectResponse.parseResponseAsMap(cancelResponse);
                        String status = (String) cancelResponseBody.get("status");
                        if (!"ACCEPT".equals(status)) {
                            String reason = extractReason(cancelResponseBody);
                            logger.warn(
                                "Prefect declined to cancel flow run '{}' (status: {}{}), the flow run may still be running on Prefect",
                                flowRunId,
                                status,
                                reason != null ? ", reason: " + reason : ""
                            );
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to cancel Prefect flow run '{}', the flow run may still be running on Prefect", flowRunId, e);
                    }
                });
        } catch (Exception e) {
            logger.warn("Failed to cancel Prefect flow run '{}', the flow run may still be running on Prefect", flowRunId, e);
        }
    }

    private static String extractReason(Map<String, Object> cancelResponseBody) {
        Object details = cancelResponseBody.get("details");
        if (details instanceof Map<?, ?> detailsMap) {
            Object reason = detailsMap.get("reason");
            return reason != null ? reason.toString() : null;
        }
        return null;
    }

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

        HttpClient httpClient = PrefectConnection.httpClient();
        trackedConnection.set(connection);
        trackedHttpClient.set(httpClient);
        trackedRunContext.set(runContext);

        String rDeploymentId = runContext.render(deploymentId).as(String.class).orElseThrow();

        // Create flow run
        logger.info("Creating flow run for deployment: {}", rDeploymentId);

        Map<String, Object> requestBody = new HashMap<>();
        if (parameters != null && !parameters.isEmpty()) {
            requestBody.put("parameters", runContext.render(parameters));
        }

        HttpRequest request = connection.request(runContext, "/deployments/" + rDeploymentId + "/create_flow_run")
            .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(requestBody)))
            .build();

        logger.debug("Sending request to: {}", request.uri());
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new Exception(
                "Failed to connect to Prefect API at " + request.uri() +
                    ". Please verify the Prefect server is running and accessible. Error: " + e.getMessage(),
                e
            );
        }
        Map<String, Object> flowRunResponse = PrefectResponse.parseResponseAsMap(response);

        String flowRunId = (String) flowRunResponse.get("id");
        logger.info("Created flow run with ID: {}", flowRunId);
        trackedFlowRunId.set(flowRunId);

        if (isCancelled.get()) {
            logger.warn("Flow run '{}' was killed while it was being created, cancelling it now", flowRunId);
            performCancel();
            throw new Exception("Flow run '" + flowRunId + "' was killed before completion and has been cancelled");
        }

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
            if (isCancelled.get()) {
                throw new Exception("Flow run '" + flowRunId + "' polling was stopped because the task was killed or the worker is shutting down");
            }

            HttpRequest statusRequest = connection.request(runContext, "/flow_runs/" + flowRunId)
                .GET()
                .build();

            HttpResponse<String> statusResponse;
            try {
                statusResponse = httpClient.send(statusRequest, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new Exception(
                    "Failed to poll flow run status from Prefect API at " + statusRequest.uri() +
                        ". Please verify the Prefect server is running and accessible. Error: " + e.getMessage(),
                    e
                );
            }
            Map<String, Object> flowRunData = PrefectResponse.parseResponseAsMap(statusResponse);

            Map<String, Object> state = (Map<String, Object>) flowRunData.get("state");
            String stateType = (String) state.get("type");
            lastKnownState.set(stateType);

            // Terminal states in Prefect
            if (isTerminalState(stateType)) {
                if ("FAILED".equals(stateType) || "CRASHED".equals(stateType) || "CANCELLED".equals(stateType)) {
                    String stateName = (String) state.get("name");
                    String message = (String) state.get("message");
                    throw new Exception(
                        "Flow run ended in state: " + stateType +
                            (stateName != null ? " (" + stateName + ")" : "") +
                            (message != null ? " - " + message : "")
                    );
                }
                return stateType;
            }

            try {
                cancelSignal.await(pollFrequency.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw interruptedFailure(runContext.logger(), flowRunId, e);
            }
        }
    }

    /**
     * Deliberately not a retryable/resumable path: an interrupted thread cannot keep polling. Names the
     * flow run, which outlives the task unless the kill already cancelled it.
     */
    private Exception interruptedFailure(Logger logger, String flowRunId, InterruptedException cause) {
        Thread.currentThread().interrupt();

        String message = "Interrupted while waiting for Prefect flow run '" + flowRunId + "'"
            + (isCancelled.get()
                ? ", the flow run was cancelled."
                : ". The flow run was not cancelled and may still be running on Prefect.");

        logger.warn(message, cause);

        return new Exception(message, cause);
    }

    private boolean isTerminalState(String stateType) {
        return stateType.equals("COMPLETED") ||
            stateType.equals("FAILED") ||
            stateType.equals("CRASHED") ||
            stateType.equals("CANCELLED");
    }

    private String getFlowRunUrl(RunContext runContext, PrefectConnection connection, String flowRunId) throws Exception {
        if (connection.isCloud()) {
            // Prefect Cloud URL format
            String rAccountId = runContext.render(connection.getAccountId()).as(String.class).orElseThrow();
            String rWorkspaceId = runContext.render(connection.getWorkspaceId()).as(String.class).orElseThrow();

            return String.format(
                "https://app.prefect.cloud/account/%s/workspace/%s/flow-runs/flow-run/%s",
                rAccountId, rWorkspaceId, flowRunId
            );
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
