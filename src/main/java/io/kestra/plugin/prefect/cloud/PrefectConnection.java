package io.kestra.plugin.prefect.cloud;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class PrefectConnection {
    private static final String PREFECT_CLOUD_API_BASE_URL = "https://api.prefect.cloud/api";
    
    @Builder.Default
    private Property<String> baseUrl = Property.of(PREFECT_CLOUD_API_BASE_URL);
    
    @NotNull
    private Property<String> apiKey;
    
    @NotNull
    private Property<String> accountId;
    
    @NotNull
    private Property<String> workspaceId;

    public HttpRequest.Builder request(RunContext runContext, String path) throws IllegalVariableEvaluationException {
        String rBaseUrl = runContext.render(baseUrl).as(String.class).orElseThrow();
        String rApiKey = runContext.render(apiKey).as(String.class).orElseThrow();
        String rAccountId = runContext.render(accountId).as(String.class).orElseThrow();
        String rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
        
        String url = rBaseUrl + 
                    "/accounts/" + rAccountId + 
                    "/workspaces/" + rWorkspaceId + 
                    path;
        
        return HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + rApiKey)
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

