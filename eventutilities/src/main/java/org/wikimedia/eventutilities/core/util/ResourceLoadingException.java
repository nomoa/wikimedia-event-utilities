package org.wikimedia.eventutilities.core.util;

import java.net.URI;

/**
 * Thrown when an error is encountered by ResourceLoader
 * while attempting to load a resource URI.
 */
public class ResourceLoadingException extends Exception {
    private final URI uri;

    public ResourceLoadingException(URI uri, String message) {
        super(message + " (resource: " + uri + ")");
        this.uri = uri;
    }

    public ResourceLoadingException(URI uri, String message, Exception cause) {
        super(message + " (resource: " + uri + ")", cause);
        this.uri = uri;
    }

    public URI getUri() {
        return this.uri;
    }
}
