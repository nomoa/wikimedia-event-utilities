package org.wikimedia.eventutilities.core.http;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.function.IntPredicate;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

/**
 * Class representing an HTTP request result from HttpRequest
 * This is not called 'response' as it also
 * can represent a failed result caused
 * by a local Exception rather than an HTTP response error status code.
 */
public class BasicHttpResult {
    /**
     * If the HTTP request that caused this result was successful.
     * If a local exception is encountered or a provided the isSuccess function
     * is false, this will be false.
     */
    protected final boolean success;

    /**
     * The HTTP response status code, or -1 if a local exception was encountered.
     */
    protected final int status;

    /**
     * The HTTP response message, or the Exception message if a local exception was encountered.
     */
    protected final String message;

    /**
     * THe HTTP response body, if there was one.
     */
    protected final byte[] body;

    /**
     * If a local IOException was encountered, this will be set to it.
     */
    protected IOException exception;

    BasicHttpResult(boolean success, int status, String message, byte[] body) {
        this.success = success;
        this.status = status;
        this.message = message;
        this.body = body;
        this.exception = null;
    }

    /**
     * Constructs a failure BasicHttpResult representing a failure due to
     * a local IOException rather than an HTTP response error status code.
     */
    BasicHttpResult(IOException e) {
        this.success = false;
        this.status = -1;
        this.message = e.getMessage();
        this.body = null;
        this.exception = e;
    }

    /**
     * Creates an BasicHttpResult from an httpcomponents HttpResponse and a lambda
     * isSuccess that determines what http response status codes constitute a successful
     * response.
     */
    public static BasicHttpResult create(HttpResponse response, IntPredicate isSuccess) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        boolean success = isSuccess.test(status);
        String message = response.getStatusLine().getReasonPhrase();
        HttpEntity responseEntity = response.getEntity();
        byte[] body = responseEntity != null ? EntityUtils.toByteArray(responseEntity) : null;

        return new BasicHttpResult(success, status, message, body);
    }

    public boolean getSuccess() {
        return success;
    }

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    /**
     * Returns a copy of the byte[] response body.
     */
    public byte[] getBody() {
        if (body == null) {
            return null;
        }
        final byte[] bodyCopy = new byte[body.length];
        System.arraycopy(body, 0, bodyCopy, 0, body.length);
        return bodyCopy;
    }

    public String getBodyAsString() {
        if (body == null) {
            return "";
        } else {
            return new String(body, UTF_8);
        }
    }

    public IOException getException() {
        return exception;
    }

    /**
     * Returns true if this result represents a failure due to a local IOException.
     */
    public boolean causedByException() {
        return exception != null;
    }

    public String toString() {

        StringBuilder repr = new StringBuilder("BasicHttpResult");

        if (success) {
            repr.append("(success) ");
        } else {
            repr.append("(failure) ");
        }

        if (causedByException()) {
            repr.append(" encountered local exception: ").append(message);
        } else {
            repr.append(" status: ").append(status).append(" message: ").append(message);
        }

        return repr.toString();
    }
}
