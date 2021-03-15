package org.wikimedia.eventutilities.core.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;


public class TestBasicHttpClient {
    private WireMockServer wireMockServer;
    private static BasicHttpClient httpClient;


    @BeforeEach
    public void startWireMock() {
        Options options = new WireMockConfiguration()
                .dynamicPort()
                .extensions(new ResponseTemplateTransformer(false));
        wireMockServer = new WireMockServer(options);
        wireMockServer.start();
        wireMockServer.stubFor(get("/test_get")
            .willReturn(aResponse()
                // Verify that request headers are set by returning it in the response body.
                .withBody(
                    "method: {{request.method}}\n" +
                    "Host: {{request.headers.Host}}\n"
                )
                .withTransformers("response-template")
            ));
        wireMockServer.stubFor(post("/test_post")
            .willReturn(aResponse()
                .withBody(
                    "method: {{request.method}}\n" +
                    "Host: {{request.headers.Host}}\n" +
                    "Body: {{request.body}}\n"
                )
                .withTransformers("response-template")
            ));
    }

    @AfterEach
    public void stopWireMock() {
        wireMockServer.stop();
    }

    public static String fileUrl = "file://" + new File("src/test/resources/event_service_to_uri.yaml").getAbsolutePath();


    @Test
    public void testGet() throws IOException {
        BasicHttpClient.Builder builder = BasicHttpClient.builder();
        httpClient = builder.build();

        String url = "http://localhost:" + wireMockServer.port() + "/test_get";
        String r = new String(httpClient.get(URI.create(url)), UTF_8);
        assertTrue(r.contains("Host: localhost"));
    }

    @Test
    public void testGetCustomRouteURL() throws IOException, MalformedURLException {
        BasicHttpClient.Builder builder = BasicHttpClient.builder();

        // http://test.host -> http://localhost:xxxxx
        builder.addRoute(
            new URL("http://test.host"),
            new URL("http://localhost:" + wireMockServer.port())
        );
        httpClient = builder.build();

        // test.host should end up with a successful request to localhost, but with Host header set to test.host
        String url = "http://test.host/test_get";
        String r = new String(httpClient.get(URI.create(url)), UTF_8);
        assertTrue(r.contains("Host: test.host"));
    }

    @Test
    public void testGetCustomRouteKeepRequestPort() throws IOException {
        BasicHttpClient.Builder builder = BasicHttpClient.builder();

        // http://test.host:xxxxx -> http://localhost:xxxxx,
        // request's port should be kept if target url does not specify one.
        builder.addRoute(
            "http://test.host:" + wireMockServer.port(),
            "http://localhost"
        );
        httpClient = builder.build();

        // test.host should end up with a successful request to localhost, but with Host header set to test.host
        String url = "http://test.host:" + wireMockServer.port() + "/test_get";
        String r = new String(httpClient.get(URI.create(url)), UTF_8);
        assertTrue(r.contains("Host: test.host"));
    }


    @Test
    public void testPost() throws IOException {
        BasicHttpClient.Builder builder = BasicHttpClient.builder();

        // http://test.host:xxxxx -> http://localhost:xxxxx,
        // request's port should be kept if target url does not specify one.
        builder.addRoute(
            "http://test.host:" + wireMockServer.port(),
            "http://localhost"
        );
        httpClient = builder.build();

        String url = "http://test.host:" + wireMockServer.port() + "/test_post";
        HttpResult r = httpClient.post(URI.create(url), "body time".getBytes(UTF_8));
        assertTrue(r.getSuccess());
        assertTrue(r.getBody().contains("Host: test.host"));
        assertTrue(r.getBody().contains("Body: body time"));
    }
}
