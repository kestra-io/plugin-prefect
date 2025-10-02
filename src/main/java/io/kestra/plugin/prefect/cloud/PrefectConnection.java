package io.kestra.plugin.prefect.cloud;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import lombok.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Map;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class PrefectConnection {
    private static final String PREFECT_CLOUD_API_BASE_URL = "https://api.prefect.cloud/api";
    
    @Builder.Default
    private Property<String> baseUrl = Property.of(PREFECT_CLOUD_API_BASE_URL);
    
    private Property<String> apiKey;
    private Property<String> accountId;
    private Property<String> workspaceId;

    public HttpRequest.Builder request(RunContext runContext, String path) throws IllegalVariableEvaluationException {
        String renderedBaseUrl = runContext.render(baseUrl).as(String.class).orElseThrow();
        String renderedApiKey = runContext.render(apiKey).as(String.class).orElseThrow();
        String renderedAccountId = runContext.render(accountId).as(String.class).orElseThrow();
        String renderedWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        
        String url = renderedBaseUrl + 
                    "/accounts/" + renderedAccountId + 
                    "/workspaces/" + renderedWorkspaceId + 
                    path;
        
        return HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + renderedApiKey)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json");
    }

    public static HttpClient httpClient() {
        return HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }
}

