package akka.stream.alpakka.marshal.json.jackson;

import static javaslang.control.Option.none;
import static javaslang.control.Option.some;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import akka.stream.alpakka.marshal.json.JSONEvent;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;

import javaslang.control.Option;
import javaslang.control.Try;

public class Jackson {
    private static final JsonFactory factory = new JsonFactory();
    
    public <T> String write(T obj, Writer<JSONEvent, T> writer) {
        return write(writer.applyAndReset(obj));
    }
    
    public <T> String writeAll(Stream<T> items, Writer<JSONEvent, T> writer) {
        try {
            StringWriter out = new StringWriter();
            JsonGenerator gen = factory.createGenerator(out);
            items.forEach(t -> {
                writeTo(gen, writer.apply(t));
            });
            writeTo(gen, writer.reset());
            gen.close();
            return out.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String write(Iterable<JSONEvent> events) {
        try {
            StringWriter out = new StringWriter();
            JsonGenerator gen = factory.createGenerator(out);
            writeTo(gen, events);
            gen.close();
            return out.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void writeTo(JsonGenerator gen, Iterable<JSONEvent> events) {
        events.forEach(evt -> {
            try {
                writeTo(gen, evt);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    public static void writeTo(JsonGenerator gen, JSONEvent evt) throws IOException {
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
    
    public <T> Stream<T> parse(String s, Reader<JSONEvent, T> reader) {
        try {
            return parse(factory.createParser(s), reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public <T> Stream<T> parse(JsonParser input, Reader<JSONEvent, T> reader) {
        reader.reset();
        
        Iterator<T> iterator = new Iterator<T>() {
            private Option<T> next = parse();
            
            private Option<T> parse() {
                for (Option<JSONEvent> evt = nextEvent(); evt.isDefined(); evt = nextEvent()) {
                    Try<T> read = reader.apply(evt.get());
                    if (read.isSuccess()) {
                        return read.toOption();
                    } else if (read.isFailure() && !ReadProtocol.isNone(read)) {
                        throw (RuntimeException) read.failed().get();
                    }
                }
                return Option.none();
            }
            
            private Option<JSONEvent> nextEvent() {
                try {
                    if (input.nextToken() != null) {
                        return some(getEvent(input));
                    } else {
                        return none(); // end of stream
                    }
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            
            @Override
            public boolean hasNext() {
                return next.isDefined();
            }

            @Override
            public T next() {
                T elmt = next.get();
                next = parse();
                return elmt;
            }
        };
        
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
            false);
    }
    
    /**
     * Returns the current token that [input] is pointing to as a JSONEvent.
     */
    public static JSONEvent getEvent(JsonParser input) throws IOException {
        switch (input.getCurrentToken()) {
        case START_OBJECT: return JSONEvent.START_OBJECT;
        case END_OBJECT: return JSONEvent.END_OBJECT;
        case START_ARRAY: return JSONEvent.START_ARRAY;
        case END_ARRAY: return JSONEvent.END_ARRAY;
        case VALUE_FALSE: return JSONEvent.FALSE;
        case VALUE_TRUE: return JSONEvent.TRUE;
        case VALUE_NULL: return JSONEvent.NULL;
        case FIELD_NAME: return new JSONEvent.FieldName(input.getCurrentName());
        case VALUE_NUMBER_FLOAT: return new JSONEvent.NumericValue(input.getValueAsString());
        case VALUE_NUMBER_INT: return new JSONEvent.NumericValue(input.getValueAsString());
        case VALUE_STRING: return new JSONEvent.StringValue(input.getValueAsString());
        default: throw new IllegalArgumentException("Unexpected token " + input.getCurrentToken() + " at " + input.getCurrentLocation());
        }
    }
    
}
