package akka.stream.alpakka.marshal.json;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import akka.NotUsed;
import akka.stream.alpakka.marshal.generic.PushPullOutputStreamAdapter;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;

/**
 * Serializes JSONEvents out as JSON by invoking Jackson, but in a non-blocking push/pull fashion,
 * only writing when there is demand from downstream.
 */
public class JacksonWriter extends PushPullOutputStreamAdapter<List<JSONEvent>, JsonGenerator> {
    private static final JsonFactory factory = new JsonFactory();
    
    /**
     * Returns a flow that buffers up to 100 JSON events and writing them together.
     */
    public static Flow<JSONEvent,ByteString,NotUsed> flow() {
        return flow(100);
    }
    
    /**
     * Returns a flow that buffers up to [maximumBatchSize] JSON events and writing them together.
     * 
     * Buffering is only done if upstream is faster than downstream. If there is demand from downstream,
     * also slower batches will be written.
     */
    public static Flow<JSONEvent,ByteString,NotUsed> flow(int maximumBatchSize) {
        return Flow.of(JSONEvent.class)
            .batch(maximumBatchSize, event -> {
                List<JSONEvent> l = new ArrayList<>();
                l.add(event);
                return l;
            }, (list, event) -> {
                list.add(event);
                return list;
            }).via(new JacksonWriter());
    }
    
    private JacksonWriter() {
        super(
            (attr, out) -> factory.createGenerator(out),
            (gen, events) -> {
                for (JSONEvent evt: events) {
                    if (evt == JSONEvent.START_OBJECT) {
                        gen.writeStartObject();
                    } else if (evt == JSONEvent.END_OBJECT) {
                        gen.writeEndObject();
                    } else if (evt == JSONEvent.START_ARRAY) {
                        gen.writeStartArray();
                    } else if (evt == JSONEvent.END_ARRAY) {
                        gen.writeEndArray();
                    } else if (evt == JSONEvent.TRUE) {
                        gen.writeBoolean(true);
                    } else if (evt == JSONEvent.FALSE) {
                        gen.writeBoolean(false);
                    } else if (evt == JSONEvent.NULL) {
                        gen.writeNull();
                    } else if (evt instanceof JSONEvent.FieldName) {
                        gen.writeFieldName(JSONEvent.FieldName.class.cast(evt).getName());
                    } else if (evt instanceof JSONEvent.StringValue) {
                        gen.writeString(JSONEvent.StringValue.class.cast(evt).getValueAsString());
                    } else if (evt instanceof JSONEvent.NumericValue) {
                        gen.writeNumber(JSONEvent.NumericValue.class.cast(evt).getValueAsString());
                    }
                }
                gen.flush();
            }
        );
    }
}
