package org.wikimedia.eventutilities.core.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import java.util.function.IntFunction;

import org.wikimedia.eventutilities.core.json.JsonLoader;


/**
 * Contains static methods that simplify making HTTP POST requests.
 */
public class HttpRequest {

    /**
     * POSTs a String to a url.
     *
     * If there is a local exception during POSTing, the HttpResult success will be false
     * and the Exception message will be in message.
     *
     * @param url
     * @param requestBody
     * @param contentType,
     * @param isSuccess
     * @return
     */
    public static HttpResult post(
        String url,
        String requestBody,
        ContentType contentType,
        IntFunction<Boolean> isSuccess
    ) {
        HttpPost httpPost = new HttpPost(url);
        HttpEntity stringEntity = new StringEntity(requestBody, contentType);
        httpPost.setEntity(stringEntity);

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            return client.execute(
                httpPost,
                // Create a custom response handler to return an HttpResult.
                response -> new HttpResult(response, isSuccess)
            );
        } catch (Exception e) {
            return new HttpResult(e);
        }
    }

    /**
     * POSTs a request body to url considering any 2xx response a successful POST.
     *
     * @param url
     * @param requestBody
     * @param contentType
     * @return
     */
    public static HttpResult post(
        String url,
        String requestBody,
        ContentType contentType
    ) {
        return post(
            url,
            requestBody,
            contentType,
            statusCode -> statusCode >= 200 && statusCode < 300
        );
    }

    /**
     * POSTs a JsonNode as a serialized JSON string to a url.
     *
     * @param url
     * @param jsonNode,
     * @param isSuccess
     * @return
     */
    public static HttpResult postJson(
        String url,
        JsonNode jsonNode,
        IntFunction<Boolean> isSuccess
    ) throws JsonProcessingException {
        String requestBody = JsonLoader.getInstance().asString(jsonNode);
        return post(url, requestBody, ContentType.APPLICATION_JSON, isSuccess);
    }

    /**
     * POSTs a JsonNode as a serialized JSON string to url
     * considering any 2xx response a successful POST.
     *
     * @param url
     * @param jsonNode
     * @return
     * @throws JsonProcessingException
     */
    public static HttpResult postJson(
        String url,
        JsonNode jsonNode
    ) throws JsonProcessingException {
        return postJson(
            url,
            jsonNode,
            statusCode -> statusCode >= 200 && statusCode < 300
        );
    }
}
