package io.kestra.plugin.prefect;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Basic test for CreateFlowRun task.
 * Note: These tests require valid Prefect Cloud credentials to run.
 * For CI/CD, you may want to mock the HTTP responses or skip these tests.
 */
@KestraTest
class CreateFlowRunTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testTaskBuild() {
        // Test that the task can be built with required parameters
        CreateFlowRun task = CreateFlowRun.builder()
            .accountId(Property.of("test-account-id"))
            .workspaceId(Property.of("test-workspace-id"))
            .deploymentId(Property.of("test-deployment-id"))
            .apiKey(Property.of("test-api-key"))
            .wait(Property.of(false))
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
            .accountId(Property.of("test-account-id"))
            .workspaceId(Property.of("test-workspace-id"))
            .deploymentId(Property.of("test-deployment-id"))
            .apiKey(Property.of("test-api-key"))
            .parameters(params)
            .wait(Property.of(true))
            .build();

        assertThat(task.getParameters(), is(notNullValue()));
        assertThat(task.getParameters().size(), is(2));
    }

    @Test
    void testConnectionBuilder() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        
        PrefectConnection connection = PrefectConnection.builder()
            .accountId(Property.of("test-account-id"))
            .workspaceId(Property.of("test-workspace-id"))
            .apiKey(Property.of("test-api-key"))
            .build();

        assertThat(connection.getAccountId(), is(notNullValue()));
        assertThat(connection.getWorkspaceId(), is(notNullValue()));
        assertThat(connection.getApiKey(), is(notNullValue()));
    }

}

