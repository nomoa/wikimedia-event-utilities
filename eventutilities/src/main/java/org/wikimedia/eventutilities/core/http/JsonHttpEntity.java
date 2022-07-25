package org.wikimedia.eventutilities.core.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.http.Header;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonHttpEntity extends AbstractHttpEntity {

    private final ObjectMapper objectMapper;
    private final JsonNode node;

    public JsonHttpEntity(ObjectMapper objectMapper, JsonNode node) {
        this.objectMapper = objectMapper;
        this.node = node;
    }

    @Override
    public Header getContentType() {
        return new BasicHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        try {
            objectMapper.writeValue(outStream, node);
            outStream.flush();
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Encountered JsonProcessingException when attempting to POST canary events", e
            );
        }
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
