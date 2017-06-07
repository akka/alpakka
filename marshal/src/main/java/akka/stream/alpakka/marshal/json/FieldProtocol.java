package akka.stream.alpakka.marshal.json;

import static akka.stream.alpakka.marshal.ReadProtocol.none;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.Writer;

import javaslang.collection.Seq;
import javaslang.collection.Vector;
import javaslang.control.Try;

/**
 * Protocol for reading and writing a single JSON field and its value (through an inner protocol)
 */
public class FieldProtocol {
    private static final Logger log = LoggerFactory.getLogger(FieldProtocol.class);
    
    public static <T> ReadProtocol<JSONEvent, T> read(String fieldName, ReadProtocol<JSONEvent, T> innerProtocol) {
        JSONEvent field = new JSONEvent.FieldName(fieldName);
        return new ReadProtocol<JSONEvent, T>() {
            @Override
            public Try<T> empty() {
                return innerProtocol.empty();
            }
            
            @Override
            public Reader<JSONEvent, T> reader() {
                return new Reader<JSONEvent, T>() {
                    private final Reader<JSONEvent, T> inner = innerProtocol.reader();
                    private boolean matched;
                    private boolean wasMatched = false;
                    private int nestedObjects = 0;
                    
                    @Override
                    public Try<T> reset() {
                        matched = false;
                        nestedObjects = 0;
                        Try<T> r = inner.reset();
                        if (wasMatched) {
                            wasMatched = false;
                            return r;
                        } else {
                            return none();
                        }
                    }
                    
                    @Override
                    public Try<T> apply(JSONEvent evt) {
                        if (!matched && nestedObjects == 0 && evt.equals(field)) {
                            matched = true;
                            wasMatched = true;
                            return none();
                        } else if (matched && nestedObjects == 0 && (evt == JSONEvent.START_OBJECT || evt == JSONEvent.START_ARRAY)) {
                            nestedObjects++;
                            return inner.apply(evt);
                        } else if (matched && nestedObjects == 1 && (evt == JSONEvent.END_OBJECT || evt == JSONEvent.END_ARRAY)) {
                            log.debug("Field ending.");
                            nestedObjects--;
                            matched = false;
                            return inner.apply(evt);
                        } else if (matched && nestedObjects == 0 && (evt instanceof JSONEvent.Value)) {
                            matched = false;
                            return inner.apply(evt);
                        } else {
                            Try<T> result = (matched) ? inner.apply(evt) : none();
                            if (evt == JSONEvent.START_OBJECT || evt == JSONEvent.START_ARRAY) {
                                nestedObjects++;
                            } else if (evt == JSONEvent.END_OBJECT || evt == JSONEvent.END_ARRAY) {
                                nestedObjects--;
                            }
                            return result;
                        }
                    }
                };
            }
            
            @Override
            public String toString() {
                return "" + field + "(" + innerProtocol + ")";
            }
        };
    }
    
    public static <T> WriteProtocol<JSONEvent, T> write(String fieldName, WriteProtocol<JSONEvent, T> innerProtocol) {
        JSONEvent field = new JSONEvent.FieldName(fieldName);
        
        return new WriteProtocol<JSONEvent, T>() {
            @Override
            public Class<? extends JSONEvent> getEventType() {
                return JSONEvent.class;
            }
            
            @Override
            public Writer<JSONEvent, T> writer() {
                Writer<JSONEvent, T> inner = innerProtocol.writer();
                return new Writer<JSONEvent, T>() {
                    Seq<JSONEvent> buffer = Vector.empty();
                    boolean fieldStarted = false;

                    @Override
                    public Seq<JSONEvent> apply(T value) {
                        if (fieldStarted) {
                            return inner.apply(value);
                        } else {
                            buffer = buffer.appendAll(inner.apply(value));
                            if (buffer.isEmpty() || buffer.eq(Vector.of(JSONEvent.START_ARRAY, JSONEvent.END_ARRAY))) {
                                return Vector.empty();
                            } else {
                                Vector<JSONEvent> result = Vector.of(field).appendAll(buffer);
                                buffer = Vector.empty();
                                fieldStarted = true;
                                return result;
                            }
                        }
                    }
                    
                    @Override
                    public Seq<JSONEvent> reset() {
                        Seq<JSONEvent> result;
                        if (fieldStarted) {
                            result = inner.reset();
                        } else {
                            buffer = buffer.appendAll(inner.reset());
                            if (buffer.isEmpty() || buffer.eq(Vector.of(JSONEvent.START_ARRAY, JSONEvent.END_ARRAY))) {
                                result = Vector.empty();
                            } else {
                                result = Vector.of(field).appendAll(buffer);
                            }
                        }
                        buffer = Vector.empty();
                        fieldStarted = false;
                        return result;
                    }

                };
            }
            
            @Override
            public String toString() {
                return "" + field + "(" + innerProtocol + ")";
            }
        };
    }
}
