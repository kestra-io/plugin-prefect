package io.kestra.plugin.prefect.cloud;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.Map;

public class PrefectResponse {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();

    public static void checkError(HttpResponse<String> response) throws IOException {
        if (response.statusCode() >= 400) {
            String errorMessage = "Prefect API error (HTTP " + response.statusCode() + "): " + response.body();
            try {
                Map<String, Object> errorBody = OBJECT_MAPPER.readValue(response.body(), Map.class);
                if (errorBody.containsKey("detail")) {
                    errorMessage = "Prefect API error (HTTP " + response.statusCode() + "): " + errorBody.get("detail");
                } else if (errorBody.containsKey("message")) {
                    errorMessage = "Prefect API error (HTTP " + response.statusCode() + "): " + errorBody.get("message");
                }
            } catch (Exception ignored) {
                // If we can't parse the error response, use the default message
            }
            throw new IOException(errorMessage);
        }
    }

    public static <T> T parseResponse(HttpResponse<String> response, Class<T> clazz) throws IOException {
        checkError(response);
        return OBJECT_MAPPER.readValue(response.body(), clazz);
    }

    public static Map<String, Object> parseResponseAsMap(HttpResponse<String> response) throws IOException {
        checkError(response);
        return OBJECT_MAPPER.readValue(response.body(), Map.class);
    }
}

