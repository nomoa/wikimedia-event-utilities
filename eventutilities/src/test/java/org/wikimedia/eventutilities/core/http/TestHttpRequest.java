package org.wikimedia.eventutilities.core.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;

public class TestHttpRequest {

    private WireMockServer wireMockServer;

    @BeforeEach
    public void startWireMock() {
        Options options = new WireMockConfiguration()
                .dynamicPort()
                .extensions(new ResponseTemplateTransformer(false));
        wireMockServer = new WireMockServer(options);
        wireMockServer.start();

        wireMockServer.stubFor(post("/test")
                .willReturn(aResponse()
                        .withBody("{{request.body}}")
                        .withTransformers("response-template")
                ));
    }

    @AfterEach
    public void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    public void postJson() throws JsonProcessingException {
        String url = "http://localhost:" + wireMockServer.port() + "/test";
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
        String url = "http://localhost:" + wireMockServer.port() + "/notfound";
        HttpResult result = HttpRequest.postJson(
            url,
            JsonNodeFactory.instance.numberNode(1234)
        );

        assertFalse(result.getSuccess());
    }
}
