package org.wikimedia.eventutilities.core.http;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntPredicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.wikimedia.eventutilities.core.util.ResourceLoader;
import org.wikimedia.utils.http.CustomRoutePlanner;

import com.google.common.collect.ImmutableMap;

/**
 * Wrapper around a {@link CloseableHttpClient} that aides in getting byte[] content at a URI.
 *
 * Supports custom host and port routing.  Usable as {@link ResourceLoader} loader function.
 *
 * NOTE: This class stores the HTTP response body in an in memory byte[] in {@link BasicHttpResult}
 * and as such should not be used for large or complex HTTP requests.
 */
@ParametersAreNonnullByDefault @ThreadSafe
public final class BasicHttpClient implements Closeable {
    /**
     * Underlying HttpClient.
     */
    private final CloseableHttpClient httpClient;

    private BasicHttpClient(@WillCloseWhenClosed CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Performs a GET request and returns the response body as a byte[].
     * If the response is not a 2xx success, or if an Exception is encountered along the way,
     * This will throw a UncheckedIOException instead.
     *
     * This function is suitable for use as a {@link ResourceLoader} loader function.
     * Call {@code resourceLoader.withHttpClient(basicHttpClient)} to have an instance
     * of ResourceLoader use a BasicHttpClient to load http and https URLs using this function.
     *
     * Note that HTTP specifies that body can exist and be empty or not exist at all. In case
     * of an existing body that is empty, this method returns an empty {@code byte[]}. In case
     * of a non-existing body, this method returns {@code null}.
     */
    @Nullable
    public byte[] getAsBytes(URI uri) {
        BasicHttpResult result = get(uri);

        if (result.getSuccess()) {
            return result.getBody();
        } else {
            String exceptionMessage = "Request to uri " + uri + " failed. " + result;
            if (result.causedByException()) {
                throw new UncheckedIOException(exceptionMessage, result.getException());
            } else {
                throw new UncheckedIOException(new IOException(exceptionMessage));
            }
        }
    }

    /**
     * Performs a GET request at URI and returns a BasicHttpResult.
     */
    @Nonnull
    public BasicHttpResult get(URI uri, IntPredicate acceptableStatus) {
        HttpUriRequest request = new HttpGet(uri);
        try (CloseableHttpResponse resp = httpClient.execute(request)) {
            return BasicHttpResult.create(resp, acceptableStatus);
        } catch (IOException e) {
            return new BasicHttpResult(e);
        }
    }

    /**
     * Performs a GET request at URI and returns a BasicHttpResult accepting any 2xx status as a success.
     */
    @Nonnull
    public BasicHttpResult get(URI uri) {
        return get(uri, BasicHttpClient::acceptableStatusPredicateDefault);
    }

    /**
     * Performs a POST request to URI and returns a BasicHttpResult.
     */
    @Nonnull
    public BasicHttpResult post(
        URI endpoint,
        byte[] postBody,
        @Nullable ContentType contentType,
        IntPredicate acceptableStatus
    ) {
        HttpPost post = new HttpPost(endpoint);
        post.setEntity(new ByteArrayEntity(postBody, contentType));
        try (CloseableHttpResponse resp = httpClient.execute(post)) {
            return BasicHttpResult.create(resp, acceptableStatus);
        } catch (IOException e) {
            return new BasicHttpResult(e);
        }
    }

    /**
     * Performs a POST request to URI and returns a BasicHttpResult accepting any 2xx status as a success.
     */
    @Nonnull
    public BasicHttpResult post(
        URI endpoint,
        byte[] data
    ) {
        return post(
            endpoint, data,
            ContentType.TEXT_PLAIN,
            BasicHttpClient::acceptableStatusPredicateDefault
        );
    }

    /**
     * Default HTTP status code predicate, if 2xx, success is true.
     */
    protected static boolean acceptableStatusPredicateDefault(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    /**
     * BasicHttpClient builder class.
     */
    @ParametersAreNonnullByDefault @NotThreadSafe
    public static class Builder {
        /**
         * Map of URL to URL.
         * Give URL might do DNS lookups when calling hashCode it's not
         * advised to use them in a map, the String representation is preferred
         * here.
         */
        private final Map<String, HttpHost> customRoutes = new HashMap<>();

        private final HttpClientBuilder clientBuilder;
        public Builder() {
            clientBuilder = HttpClientBuilder.create();
        }

        @Nonnull
        public HttpClientBuilder httpClientBuilder() {
            return clientBuilder;
        }

        /**
         * Adds a custom route from sourceURL to targetURL's host, port and protocol.
         * That is, if a request is made to sourceURL, targetURL's host port and protocol will be used instead.
         * If targetURL does not have a port defined, sourceURL's port will be used.
         */
        @Nonnull
        public Builder addRoute(String sourceURL, String targetURL) throws MalformedURLException {
            this.customRoutes.put(new URL(sourceURL).getHost(), HttpHost.create(targetURL));
            return this;
        }

        /**
         * Add custom route using URLs instead of STrings.
         */
        @Nonnull
        public Builder addRoute(URL sourceURL, URL targetURL) {
            customRoutes.put(sourceURL.getHost(), new HttpHost(targetURL.getHost(), targetURL.getPort(), targetURL.getProtocol()));
            return this;
        }

        @Nonnull
        public BasicHttpClient build() {
            if (!customRoutes.isEmpty()) {
                clientBuilder.setRoutePlanner(
                    new CustomRoutePlanner(
                        ImmutableMap.copyOf(customRoutes),
                        new DefaultRoutePlanner(DefaultSchemePortResolver.INSTANCE)
                    )
                );
            }
            return new BasicHttpClient(clientBuilder.build());
        }
    }

}
