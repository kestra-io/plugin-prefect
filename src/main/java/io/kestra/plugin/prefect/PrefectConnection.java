package io.kestra.plugin.prefect;

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
    private Property<String> apiUrl = Property.of(PREFECT_CLOUD_API_BASE_URL);
    
    // Optional: only required for Prefect Cloud (not used in self-hosted)
    private Property<String> apiKey;
    
    // Optional: only required for Prefect Cloud
    private Property<String> accountId;
    
    // Optional: only required for Prefect Cloud
    private Property<String> workspaceId;

    public HttpRequest.Builder request(RunContext runContext, String path) throws IllegalVariableEvaluationException {
        String rApiUrl = runContext.render(apiUrl).as(String.class).orElseThrow();
        
        // Build URL - for Prefect Cloud, include account and workspace in path
        // For self-hosted, use the API URL directly
        String url;
        if (accountId != null && workspaceId != null) {
            // Prefect Cloud mode
            String rAccountId = runContext.render(accountId).as(String.class).orElseThrow();
            String rWorkspaceId = runContext.render(workspaceId).as(String.class).orElseThrow();
            url = rApiUrl + 
                  "/accounts/" + rAccountId + 
                  "/workspaces/" + rWorkspaceId + 
                  path;
        } else {
            // Self-hosted mode
            url = rApiUrl + path;
        }
        
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json");
        
        // Add Authorization header only if API key is provided
        // - Prefect Cloud: Use Bearer token authentication
        // - Self-hosted: Use Basic authentication (base64-encoded "admin:pass")
        if (apiKey != null) {
            String rApiKey = runContext.render(apiKey).as(String.class).orElse(null);
            if (rApiKey != null && !rApiKey.isEmpty()) {
                if (isCloud()) {
                    // Prefect Cloud uses Bearer token
                    builder.header("Authorization", "Bearer " + rApiKey);
                } else {
                    // Self-hosted Prefect uses Basic authentication
                    // apiKey should contain base64-encoded "admin:pass"
                    // If it doesn't start with "Basic ", prepend it
                    if (rApiKey.startsWith("Basic ")) {
                        builder.header("Authorization", rApiKey);
                    } else {
                        builder.header("Authorization", "Basic " + rApiKey);
                    }
                }
            }
        }
        
        return builder;
    }
    
    public boolean isCloud() {
        return accountId != null && workspaceId != null;
    }

    public static HttpClient httpClient() {
        return HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }
}

