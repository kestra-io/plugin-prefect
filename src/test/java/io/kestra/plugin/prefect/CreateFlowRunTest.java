package io.kestra.plugin.prefect;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Basic test for CreateFlowRun task.
 * Note: These tests require valid Prefect Cloud credentials to run.
 * For CI/CD, you may want to mock the HTTP responses or skip these tests.
 */
@KestraTest
class CreateFlowRunTest {
    private static final String DEPLOYMENT_ID = "test-deployment-id";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testTaskBuild() {
        // Test that the task can be built with required parameters
        CreateFlowRun task = CreateFlowRun.builder()
            .accountId(Property.ofValue("test-account-id"))
            .workspaceId(Property.ofValue("test-workspace-id"))
            .deploymentId(Property.ofValue("test-deployment-id"))
            .apiKey(Property.ofValue("test-api-key"))
            .wait(Property.ofValue(false))
            .build();

        assertThat(task.getAccountId(), is(notNullValue()));
        assertThat(task.getWorkspaceId(), is(notNullValue()));
        assertThat(task.getDeploymentId(), is(notNullValue()));
        assertThat(task.getApiKey(), is(notNullValue()));
    }

    @Test
    void testTaskBuildWithParameters() {
        // Test that the task can be built with parameters
        Map<String, Object> params = Map.of(
            "param1", "value1",
            "param2", 42
        );

        CreateFlowRun task = CreateFlowRun.builder()
            .accountId(Property.ofValue("test-account-id"))
            .workspaceId(Property.ofValue("test-workspace-id"))
            .deploymentId(Property.ofValue("test-deployment-id"))
            .apiKey(Property.ofValue("test-api-key"))
            .parameters(params)
            .wait(Property.ofValue(true))
            .build();

        assertThat(task.getParameters(), is(notNullValue()));
        assertThat(task.getParameters().size(), is(2));
    }

    @Test
    void testConnectionBuilder() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        PrefectConnection connection = PrefectConnection.builder()
            .accountId(Property.ofValue("test-account-id"))
            .workspaceId(Property.ofValue("test-workspace-id"))
            .apiKey(Property.ofValue("test-api-key"))
            .build();

        assertThat(connection.getAccountId(), is(notNullValue()));
        assertThat(connection.getWorkspaceId(), is(notNullValue()));
        assertThat(connection.getApiKey(), is(notNullValue()));
    }

    @Test
    void killCancelsRunningFlowRunPromptlyAndIsIdempotent() throws Exception {
        String flowRunId = UUID.randomUUID().toString();
        AtomicInteger getRunsCounter = new AtomicInteger();
        List<String> setStateBodies = new CopyOnWriteArrayList<>();
        HttpServer server = startStubServer(flowRunId, getRunsCounter, setStateBodies, "RUNNING");

        try {
            CreateFlowRun task = flowRunTask(server, Duration.ofSeconds(30));
            RunContext runContext = runContextFactory.of(Map.of());

            CompletableFuture<Exception> runOutcome = runAsync(task, runContext);

            waitUntil(() -> getRunsCounter.get() >= 1, Duration.ofSeconds(5));

            long start = System.nanoTime();
            task.kill();
            Exception thrown = runOutcome.get(5, TimeUnit.SECONDS);
            long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

            assertThat("kill() should interrupt the poll loop instead of waiting out pollFrequency", elapsedMs, lessThan(30_000L));
            assertThat(thrown, is(notNullValue()));
            assertThat(setStateBodies, hasSize(1));
            assertThat(setStateBodies.get(0), containsString("CANCELLING"));

            // A second kill() must not issue a second cancel request.
            task.kill();
            assertThat(setStateBodies, hasSize(1));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void stopCancelsPromptlyWithoutBlocking() throws Exception {
        String flowRunId = UUID.randomUUID().toString();
        AtomicInteger getRunsCounter = new AtomicInteger();
        List<String> setStateBodies = new CopyOnWriteArrayList<>();
        HttpServer server = startStubServer(flowRunId, getRunsCounter, setStateBodies, "RUNNING");

        try {
            CreateFlowRun task = flowRunTask(server, Duration.ofSeconds(30));
            RunContext runContext = runContextFactory.of(Map.of());

            CompletableFuture<Exception> runOutcome = runAsync(task, runContext);

            waitUntil(() -> getRunsCounter.get() >= 1, Duration.ofSeconds(5));

            long start = System.nanoTime();
            task.stop();
            long stopElapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

            assertThat("stop() must be non-blocking", stopElapsedMs, lessThan(2_000L));

            Exception thrown = runOutcome.get(5, TimeUnit.SECONDS);
            assertThat(thrown, is(notNullValue()));
            assertThat(setStateBodies, hasSize(1));
            assertThat(setStateBodies.get(0), containsString("CANCELLING"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void cancellingStateIsNotReportedAsTerminal() throws Exception {
        String flowRunId = UUID.randomUUID().toString();
        AtomicInteger getRunsCounter = new AtomicInteger();
        List<String> setStateBodies = new CopyOnWriteArrayList<>();
        HttpServer server = startStubServer(flowRunId, getRunsCounter, setStateBodies, "CANCELLING");

        try {
            CreateFlowRun task = flowRunTask(server, Duration.ofMillis(100));
            RunContext runContext = runContextFactory.of(Map.of());

            CompletableFuture<Exception> runOutcome = runAsync(task, runContext);

            // If CANCELLING were (incorrectly) terminal, run() would already have completed successfully
            // after the first poll. Observing several polls with the task still running proves it keeps
            // waiting for a real terminal state instead.
            waitUntil(() -> getRunsCounter.get() >= 3, Duration.ofSeconds(5));
            assertThat(runOutcome.isDone(), is(false));

            task.kill();
            Exception thrown = runOutcome.get(5, TimeUnit.SECONDS);
            assertThat(thrown, is(notNullValue()));
        } finally {
            server.stop(0);
        }
    }

    private CreateFlowRun flowRunTask(HttpServer server, Duration pollFrequency) {
        return CreateFlowRun.builder()
            .apiUrl(Property.ofValue("http://localhost:" + server.getAddress().getPort() + "/api"))
            .deploymentId(Property.ofValue(DEPLOYMENT_ID))
            .wait(Property.ofValue(true))
            .pollFrequency(pollFrequency)
            .build();
    }

    private static CompletableFuture<Exception> runAsync(CreateFlowRun task, RunContext runContext) {
        CompletableFuture<Exception> outcome = new CompletableFuture<>();
        Thread runnerThread = new Thread(() -> {
            try {
                task.run(runContext);
                outcome.complete(null);
            } catch (Exception e) {
                outcome.complete(e);
            }
        });
        runnerThread.setDaemon(true);
        runnerThread.start();
        return outcome;
    }

    private static HttpServer startStubServer(
        String flowRunId,
        AtomicInteger getRunsCounter,
        List<String> setStateBodies,
        String polledStateType
    ) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);

        server.createContext("/api/deployments/" + DEPLOYMENT_ID + "/create_flow_run", exchange ->
            sendJson(exchange, 200, "{\"id\":\"" + flowRunId + "\",\"state\":{\"type\":\"SCHEDULED\",\"name\":\"Scheduled\"}}"));

        server.createContext("/api/flow_runs/" + flowRunId, exchange -> {
            if (exchange.getRequestURI().getPath().endsWith("/set_state")) {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                setStateBodies.add(body);
                sendJson(exchange, 200, "{\"status\":\"ACCEPT\",\"state\":{\"type\":\"CANCELLING\",\"name\":\"Cancelling\"}}");
            } else {
                getRunsCounter.incrementAndGet();
                sendJson(
                    exchange,
                    200,
                    "{\"id\":\"" + flowRunId + "\",\"state\":{\"type\":\"" + polledStateType + "\",\"name\":\"" + polledStateType + "\"}}"
                );
            }
        });

        server.start();
        return server;
    }

    private static void sendJson(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void waitUntil(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Condition not met within " + timeout);
            }
            Thread.sleep(50);
        }
    }
}
