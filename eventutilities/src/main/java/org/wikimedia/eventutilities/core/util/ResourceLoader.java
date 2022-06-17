package org.wikimedia.eventutilities.core.util;

import static com.google.common.collect.ImmutableList.toImmutableList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikimedia.eventutilities.core.http.BasicHttpClient;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

/**
 * Loads resource content at URIs.
 * How the resource is loaded depends on the configured loader functions for
 * a URL protocol scheme.
 *
 * Usage:
 *
 * <pre>{@code
 *     ResourceLoader resourceLoader = ResourceLoader.builder()
 *          .setBaseUrls(Arrays.asList(
 *              new URL
 *
 * }</pre>
 */
public class ResourceLoader {
    /**
     * Map of scheme/protocool to loader function.
     */
    private final Map<String, Function<URI, byte[]>> loaders;

    /**
     * Default loader function used if a URI's scheme is not in loaders map.
     */
    private final Function<URI, byte[]> defaultLoader;

    /**
     * Base URLs to resolve relative URIs in.
     */
    private final List<URL> baseUrls;

    private static final Logger log = LoggerFactory.getLogger(ResourceLoader.class.getName());


    /**
     *
     * @param loaders
     *  Map of URI protocol/scheme (e.g. file, http, https, etc.) to
     *  {@link Function} that takes a {@link URI} and returns a byte[] of the content at the URI.
     *
     * @param defaultLoader
     *  Default loader to use if no loader exists for the URI's scheme.
     *
     * @param baseUrls
     *  baseUrls prefixes that act like a relative URI search path.
     *  When loading a uri, if that uri is relative, each baseUrl will
     *  be prefixed to it and then attempted to be loaded.
     *  Whichever baseUrl + uri successfully loads first will be returned.
     */
    public ResourceLoader(
        Map<String, Function<URI, byte[]>> loaders,
        Function<URI, byte[]> defaultLoader,
        List<URL> baseUrls
    ) {
        this.loaders = loaders;
        this.defaultLoader = defaultLoader;
        this.baseUrls = baseUrls;
    }

    /**
     * Loads the resource at uri, potentially prefixing relative URIs with baseUrls.
     *
     * @param uri {@link URI}
     * @return contents as bute[] at uri
     */
    public byte[] load(URI uri) throws ResourceLoadingException {
        return loadFirst(getPossibleResourceUris(uri));
    }

    /**
     * Calls the loader function for the uri's scheme.
     * If the uri is not absolute, or if no loader for the uri scheme exists,
     * this will use the defaultLoader.
     */
    private byte[] fetch(URI uri) {
        Function<URI, byte[]> loader = defaultLoader;
        if (uri.isAbsolute() && loaders.containsKey(uri.getScheme())) {
            loader = loaders.get(uri.getScheme());
        }
        return loader.apply(uri);
    }

    /**
     * Attempts to fetch the content at each uri, and returns the first fetch result that succeeds.
     */
    @SuppressWarnings("checkstyle:IllegalCatch")
    private byte[] loadFirst(List<URI> uris) throws ResourceLoadingException {
        byte[] content = null;
        List<ResourceLoadingException> loadingExceptions = new ArrayList<>();

        for (URI uri: uris) {
            try {
                content = this.fetch(uri);
                break;
            } catch (Exception e) {
                loadingExceptions.add(new ResourceLoadingException(uri, "Failed loading resource.", e));
            }
        }

        if (content != null) {
            return content;
        } else {
            // If we failed loading a schema but we encountered any ResourceLoadingException
            // while trying, log them all but only throw the first one.
            if (!loadingExceptions.isEmpty()) {
                for (ResourceLoadingException e: loadingExceptions) {
                    log.error("Caught exception when trying to load resource.", e);
                }
                throw loadingExceptions.get(0);
            } else {
                // This shouldn't happen, as content should not be null if loadingExceptions is empty.
                throw new RuntimeException(this + " failed loading resource in list of URIs: " + uris);
            }
        }
    }

    /**
     * If the uri is aboslute, or if no baseUris are set, the only possible
     * uri is the provided one.  Else, the uri will be prefixed with each of the baseUris.
     */
    public List<URI> getPossibleResourceUris(URI uri) {
        if (uri.isAbsolute() || baseUrls.isEmpty()) {
            return ImmutableList.of(uri);
        } else {
            return baseUrls.stream().map(baseUrl -> {
                try {
                    return new URI(baseUrl.toString() + uri.toString());
                } catch (java.net.URISyntaxException e) {
                    throw new IllegalArgumentException(
                        "Failed building new URI with " + baseUrl + " + " + uri + ". ", e
                    );
                }
            }).collect(toImmutableList());
        }
    }

    /**
     * Returns a ResourceLoader.Builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for ResouceLoader.
     */
    public static class Builder {
        private Map<String, Function<URI, byte[]>> loaders = new HashMap<>();
        private Function<URI, byte[]> defaultLoader;
        private List<URL> baseUrls = new ArrayList<>();

        public Builder() {
            defaultLoader = uri -> {
                try {
                   return Resources.toByteArray(uri.toURL());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        }

        /**
         * Adds a loader function for a URI scheme.
         */
        public Builder addLoader(String scheme, Function<URI, byte[]> loader) {
            loaders.put(scheme, loader);
            return this;
        }

        /**
         * Sets the default loader. If this is not called, the defaultLoader
         * will use {@link Resources}.toByteArray.
         */
        public Builder setDefaultLoader(Function<URI, byte[]> loader) {
            defaultLoader = loader;
            return this;
        }

        // TODO: Shouldn't we always build with an httpClient by default?

        /**
         * Adds loaders for http and https using {@link BasicHttpClient}.
         */
        public Builder withHttpClient(BasicHttpClient httpClient) {
            loaders.put("http", httpClient::getAsBytes);
            loaders.put("https", httpClient::getAsBytes);
            return this;
        }

        /**
         * Adds loaders for http and https using a default {@link BasicHttpClient}.
         */
        public Builder withHttpClient() {
            BasicHttpClient httpClient = BasicHttpClient.builder().build();
            return withHttpClient(httpClient);
        }

        /**
         * Sets the baseUrls.
         */
        public Builder setBaseUrls(List<URL> baseUrls) {
            this.baseUrls = baseUrls;
            return this;
        }

        public ResourceLoader build() {
            return new ResourceLoader(ImmutableMap.copyOf(loaders), defaultLoader, baseUrls);
        }
    }

    /**
     * Helper function to convert a String url to a URL.
     */
    public static URL asURL(String u) {
        try {
            return new URL(u);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("baseUrl string " + u +  "could be converted to URL.", e);
        }
    }

    /**
     * Helper function to convert a List of String urls to URLs.
     */
    public static List<URL> asURLs(Collection<String> baseUrls) {
        return baseUrls.stream().map(ResourceLoader::asURL).collect(toImmutableList());
    }


    public String toString() {
        return "ResourceLoader([" + String.join(",", baseUrls.toString()) + "])";
    }
}

