package org.wikimedia.eventutilities.core.util;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.io.Resources;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;


class TestResourceLoader {
    private static final String fileRelativePath = "src/test/resources/event_service_to_uri.yaml";
    private static final String fileAbsolutePath =
        "file://" + new File(fileRelativePath).getAbsolutePath();

    private static String expectedContent;

    @BeforeAll
    static void beforeAll() throws IOException {
        expectedContent = new String(Resources.toByteArray(new URL(fileAbsolutePath)), UTF_8);
    }

    @Test
    public void testAbsoluteFilePath() throws ResourceLoadingException {
        ResourceLoader loader = ResourceLoader.builder().build();

        assertEquals(
            new String(loader.load(URI.create(fileAbsolutePath)), UTF_8),
            expectedContent,
            "Should load absolute file:/// uri"
        );
    }

    @Test
    public void testRelativeFilePath() throws ResourceLoadingException, IOException {
        ResourceLoader loader = ResourceLoader.builder()
            .setBaseUrls(
                ResourceLoader.asURLs(Collections.singletonList("file://" + new File("./").getCanonicalPath()))
            )
            .build();

        assertEquals(
            new String(loader.load(URI.create("/" + fileRelativePath)), UTF_8),
            expectedContent,
            "Should load relative file:// uri from baseUris"
        );
    }

    @Test
    public void testNoBaseUrisRelativePath() {
        ResourceLoader loader = ResourceLoader.builder().build();

        assertThrows(
            ResourceLoadingException.class,
            () -> loader.load(URI.create(fileRelativePath)),
            "A relative URI with no base URIs should throw"
        );
    }

    @Test
    public void testLoadFirst() throws ResourceLoadingException {
        Options options = new WireMockConfiguration()
            .dynamicPort()
            .extensions(new ResponseTemplateTransformer(false));
        WireMockServer wireMockServer = new WireMockServer(options);
        wireMockServer.start();
        wireMockServer.stubFor(get("/directory/test_get")
            .willReturn(
                aResponse()
                // Verify that request headers are set by returning it in the response body.
                .withBody(
                    "path: {{request.path}}\n"
                )
                .withTransformers("response-template")
            ));
        wireMockServer.stubFor(get(urlPathMatching("/nonexistent/test_get"))
            .willReturn(notFound()));

        // This resource loader should succeed looking up relative /test_get
        // at localhost:xxxxx/directory/test_get.
        // The /nonexistent path will fail, but the /directory one should succeed.
        ResourceLoader loader = ResourceLoader.builder()
            .withHttpClient()
            .setBaseUrls(ResourceLoader.asURLs(Arrays.asList(
                "http://localhost:" + wireMockServer.port() + "/nonexistent",
                "http://localhost:" + wireMockServer.port() + "/directory"
            )))
            .build();

        String body = new String(loader.load(URI.create("/test_get")), UTF_8);
        wireMockServer.stop();

        assertTrue(body.contains("path: /directory/test_get"));
    }

}
