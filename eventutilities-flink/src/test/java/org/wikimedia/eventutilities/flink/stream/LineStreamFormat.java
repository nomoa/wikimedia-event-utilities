package org.wikimedia.eventutilities.flink.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

// NOTE: LineStreamFormat was removed from the main source during code review.
// This is used by fileStreamSourceBuilder method in TestEventDataStreamFactory.
// If we decide we want to promote fileStreamSourceBuilder back into the main EventDataStreamFactory
// in the future, we can move this class into main source then.

/**
 * A reader format that reads text lines from a file, and uses the provided DeserializationSchema to deserialize them.
 *
 * <p>The reader uses Java's built-in {@link InputStreamReader} to decode the byte stream using
 * StandardCharsets.UTF_8.
 *
 * NOTE: This class is based on Flink 1.15.0's TextLineInputFormat.
 *
 * <p>This format does not support optimized recovery from checkpoints. On recovery, it will re-read
 * and discard the number of lines that were processed before the last checkpoint. That is due to
 * the fact that the offsets of lines in the file cannot be tracked through the charset decoders
 * with their internal buffering of stream input and charset decoder state.
 *
 */
public class LineStreamFormat<T> extends SimpleStreamFormat<T> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<T> deserializationSchema;

    public LineStreamFormat(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public Reader<T> createReader(Configuration config, FSDataInputStream stream) {
        final BufferedReader reader =
            new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        return new Reader<T>(
            reader,
            deserializationSchema
        );
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // ------------------------------------------------------------------------

    /** The actual reader for the {@code LineStreamFormat}. */
    private static final class Reader<T> implements StreamFormat.Reader<T> {

        private final BufferedReader reader;
        private final DeserializationSchema<T> deserializationSchema;

        Reader(final BufferedReader reader, DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            this.reader = reader;
        }

        @Nullable
        @Override
        public T read() throws IOException {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            return deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
