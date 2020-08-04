package org.wikimedia.eventutilities.core.http;

import java.io.IOException;
import java.util.function.IntFunction;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

/**
 * POJO representing an HTTP request result from HttpRequest
 * This is not called 'response' as it also
 * can represent a failed result caused
 * by a local Exception rather than an HTTP response error status code.
 */
public class HttpResult {
    /**
     * If the HTTP request that caused this result was successful.
     * If a local exception is encountered or a provided the isSuccess function
     * is false, this will be false.
     */
    protected Boolean success;

    /**
     * The HTTP response status code, or null if a local exception was encountered.
     */
    protected Integer status;

    /**
     * The HTTP response message, or the Exception message if a local exeaption was encountered.
     */
    protected String message;

    /**
     * THe HTTP response body, if there was one.
     */
    protected String body;

    /**
     * If a local exception was encountered, this will be set to it.
     */
    protected Exception exception;

    /**
     * @param success
     * @param status
     * @param message
     * @param body
     */
    HttpResult(boolean success, int status, String message, String body) {
        this.success = success;
        this.status = status;
        this.message = message;
        this.body = body;
        this.exception = null;
    }

    /**
     * Constructs an HttpResult from an httpcomponents HttpResponse and a lambda
     * isSuccess that determines what http response status codes constitute a successful
     * response.
     * @param response
     * @param isSuccess
     * @throws IOException
     */
    HttpResult(HttpResponse response, IntFunction<Boolean> isSuccess) throws IOException {
        this.status = response.getStatusLine().getStatusCode();
        this.success = isSuccess.apply(status);
        this.message = response.getStatusLine().getReasonPhrase();

        HttpEntity responseEntity = response.getEntity();
        if (responseEntity != null) {
            this.body = EntityUtils.toString(responseEntity);
        } else {
            this.body = null;
        }

        this.exception = null;
    }

    /**
     * Constructs a failure HttpResult representing a failure due to
     * a local Exception rather than an HTTP response error status code.
     * @param e
     */
    HttpResult(Exception e) {
        this.success = false;
        this.status = null;
        this.message = e.getMessage();
        this.body = null;
        this.exception = e;
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

    public String getBody() {
        return body;
    }

    public Exception getException() {
        return exception;
    }

    /**
     * Returns true if this result represents a failure due to a local Exception.
     * @return
     */
    public boolean causedByException() {
        return exception != null;
    }
}
