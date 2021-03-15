package org.wikimedia.eventutilities.core.http;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntPredicate;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

/**
 * Wrapper around a {@link CloseableHttpClient} that aides in getting byte[] content at a URI.
 *
 * Supports custom host and port routing.  Usable as {@link ResourceLoader} loader function.
 */
public class BasicHttpClient {
    /**
     * Underlying HttpClient.
     */
    private final CloseableHttpClient httpClient;

    public BasicHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a function suitable for use as a {@link ResourceLoader} loader function.
     * Call <code>resourceLoaderr.withHttpClient(basicHttpClient)</code> to have an instance
     * of ResourceLoader use a BasicHttpClient to load http and https URLs.
     */
    public Function<URI, byte[]> asResourceLoader() {
        return uri -> {
            try {
                return get(uri);
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        };
    }

    /**
     * Gets the bytes at uri.
     */
    public byte[] get(URI uri) throws IOException {
        HttpUriRequest request = new HttpGet(uri);
        try (CloseableHttpResponse resp = httpClient.execute(request)) {
            return EntityUtils.toByteArray(resp.getEntity());
        }
    }

    /**
     * TODO: Perhaps remove this method, it is only used by CanaryEventProducer.
     */
    public HttpResult post(
        URI endpoint,
        byte[] data,
        ContentType contentType,
        IntPredicate acceptableStatus
    ) throws IOException {
        HttpPost post = new HttpPost(endpoint);
        post.setEntity(new ByteArrayEntity(data, contentType));
        try (CloseableHttpResponse resp = httpClient.execute(post)) {
            return new HttpResult(
                acceptableStatus.test(resp.getStatusLine().getStatusCode()),
                resp.getStatusLine().getStatusCode(),
                resp.getStatusLine().getReasonPhrase(),
                EntityUtils.toString(resp.getEntity())
            );
        }
    }

    public HttpResult post(
        URI endpoint,
        byte[] data
    ) throws IOException {
        return post(
            endpoint, data,
            ContentType.TEXT_PLAIN,
            statusCode -> statusCode >= 200 && statusCode < 300
        );
    }

    /**
     * BasicHtttpClient builder class.
     */
    public static class Builder {
        /**
         * Map of URL to URL.
         * Give URL might do DNS lookups when calling hashCode it's not
         * advised to use them in a map, the String representation is preferred
         * here.
         */
        private final Map<String, URL> customRoutes = new HashMap<>();

        private final HttpClientBuilder clientBuilder;
        public Builder() {
            clientBuilder = HttpClientBuilder.create();
        }

        public HttpClientBuilder httpClientBuilder() {
            return clientBuilder;
        }

        /**
         * Adds a custom route from sourceURL to targetURL's host, port and protocol.
         * That is, if a request is made to sourceURL, targetURL's host port and protocol will be used instead.
         * If targetURL does not have a port defined, sourceURL's port will be used.
         */
        public Builder addRoute(String sourceURL, String targetURL) throws MalformedURLException {
            addRoute(new URL(sourceURL), new URL(targetURL));
            return this;
        }

        /**
         * Add custom route using URLs instead of STrings.
         */
        public Builder addRoute(URL sourceURL, URL targetURL) {
            customRoutes.put(sourceURL.toExternalForm(), targetURL);
            return this;
        }

        public BasicHttpClient build() {
            if (!customRoutes.isEmpty()) {
                clientBuilder.setRoutePlanner(
                    new CustomRoute(
                        Collections.unmodifiableMap(customRoutes),
                        new DefaultRoutePlanner(DefaultSchemePortResolver.INSTANCE)
                    )
                );
            }
            return new BasicHttpClient(clientBuilder.build());
        }
    }

    /**
     * Used if BasicHttpClient.Builder is used to add custom routes.
     */
    private static class CustomRoute implements HttpRoutePlanner {
        private final Map<String, URL> routes;
        private final HttpRoutePlanner defaultRoutePlanner;

        CustomRoute(Map<String, URL> routes, HttpRoutePlanner defaultRoutePlanner) {
            this.routes = routes;
            this.defaultRoutePlanner = defaultRoutePlanner;
        }

        /**
         * Gets a string URI form of requestHost. If this exists in routes,
         * routes value will be used to create a new HttpRoute to the target route value.
         */
        @Override
        public HttpRoute determineRoute(
            HttpHost requestHost,
            HttpRequest request,
            HttpContext context
        ) throws HttpException {
            String requestURL;
            requestURL = requestHost.toURI();

            URL targetURL = routes.get(requestURL);
            if (targetURL != null) {
                return new HttpRoute(
                    new HttpHost(
                        targetURL.getHost(),
                        // If the target port was not set, then assume we want use the same one as the request url.
                        targetURL.getPort() != -1 ? targetURL.getPort() : requestHost.getPort(),
                        targetURL.getProtocol()
                    )
                );
            } else {
                return defaultRoutePlanner.determineRoute(requestHost, request, context);
            }
        }
    }
}
