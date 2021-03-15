package org.wikimedia.eventutilities.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.io.Resources;

public class TestResourceLoader {

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

}
