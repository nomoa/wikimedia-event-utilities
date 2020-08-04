package org.wikimedia.eventutilities.core.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TestHttpRequest {

    private static HttpServer httpServer;
    private static InetSocketAddress httpServerAddress;

    private static HttpServer createTestHttpServer() throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

        httpServer.createContext("/test", exchange -> {
            String requestBody;
            try (InputStream in = exchange.getRequestBody()) {
                requestBody = IOUtils.toString(in, StandardCharsets.UTF_8);
            }

            byte[] response = requestBody.getBytes();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });

        return httpServer;
    }

    @BeforeAll
    public static void setUp() throws IOException {
        httpServer = createTestHttpServer();
        httpServerAddress = httpServer.getAddress();
        httpServer.start();
    }

    @AfterAll
    public static void tearDown() {
        httpServer.stop(0);
    }

    @Test
    public void postJson() throws JsonProcessingException {
        String url = "http://" + httpServerAddress.getHostString() + ":" + httpServerAddress.getPort() + "/test";
        HttpResult result = HttpRequest.postJson(
            url,
            JsonNodeFactory.instance.numberNode(1234)
        );

        assertTrue(result.getSuccess());
        assertEquals(200, result.getStatus());
        assertEquals("1234", result.getBody());
    }

    @Test
    public void postJsonHttpFailureResponse() throws JsonProcessingException {
        String url = "http://" + httpServerAddress.getHostString() + ":" + httpServerAddress.getPort() + "/notfound";
        HttpResult result = HttpRequest.postJson(
            url,
            JsonNodeFactory.instance.numberNode(1234)
        );

        assertFalse(result.getSuccess());
    }
}
