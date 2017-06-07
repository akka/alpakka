package akka.stream.alpakka.marshal.json;

import static akka.stream.alpakka.marshal.ReadProtocol.none;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.Writer;

import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.collection.Vector;
import javaslang.control.Option;
import javaslang.control.Try;

/**
 * Protocol for reading and writing individual JSON fields and their value (through an inner protocol), mapping each field's name
 * as a String, and delegating to an inner protocol for the value.
 */
public class AnyFieldProtocol {
    private static final Logger log = LoggerFactory.getLogger(AnyFieldProtocol.class);
    
    public static <T> ReadProtocol<JSONEvent,Tuple2<String,T>> read(ReadProtocol<JSONEvent,T> innerProtocol) {
        
        return new ReadProtocol<JSONEvent,Tuple2<String,T>>() {
            @Override
            public Reader<JSONEvent, Tuple2<String, T>> reader() {
                return new Reader<JSONEvent, Tuple2<String,T>>() {
                    private final Reader<JSONEvent, T> inner = innerProtocol.reader();
                    private boolean matched;
                    private int nestedObjects = 0;
                    private Option<String> lastField = Option.none();
                    
                    @Override
                    public Try<Tuple2<String,T>> reset() {
                        matched = false;
                        nestedObjects = 0;
                        Try<T> i = inner.reset();
                        Try<Tuple2<String,T>> result = tuple(i);
                        lastField = Option.none();
                        return result;
                    }

                    private Try<Tuple2<String, T>> tuple(Try<T> i) {
                        return lastField.isDefined() ? i.map(t -> Tuple.of(lastField.get(), t)) : none();
                    }
                    
                    @Override
                    public Try<Tuple2<String,T>> apply(JSONEvent evt) {
                        if (!matched && nestedObjects == 0 && evt instanceof JSONEvent.FieldName) {
                            matched = true;
                            lastField = Option.some(JSONEvent.FieldName.class.cast(evt).getName());
                            log.debug("AnyField started: {}", lastField);
                            return none();
                        } else if (matched && nestedObjects == 0 && (evt == JSONEvent.START_OBJECT || evt == JSONEvent.START_ARRAY)) {
                            nestedObjects++;
                            return tuple(inner.apply(evt));
                        } else if (matched && nestedObjects == 1 && (evt == JSONEvent.END_OBJECT || evt == JSONEvent.END_ARRAY)) {
                            log.debug("AnyField ending nested.");
                            nestedObjects--;
                            matched = false;
                            return tuple(inner.apply(evt));
                        } else if (matched && nestedObjects == 0 && (evt instanceof JSONEvent.Value)) {
                            log.debug("AnyField ending value.");
                            matched = false;
                            return tuple(inner.apply(evt));
                        } else {
                            Try<T> result = (matched) ? inner.apply(evt) : none();
                            if (evt == JSONEvent.START_OBJECT || evt == JSONEvent.START_ARRAY) {
                                nestedObjects++;
                            } else if (evt == JSONEvent.END_OBJECT || evt == JSONEvent.END_ARRAY) {
                                nestedObjects--;
                            }
                            return tuple(result);
                        }
                    }
                };
            }
            
            @Override
            public String toString() {
                return "(any): " + innerProtocol;
            }
        };
    }
    
    public static <T> WriteProtocol<JSONEvent, Tuple2<String,T>> write(WriteProtocol<JSONEvent, T> innerProtocol) {
        return new WriteProtocol<JSONEvent, Tuple2<String,T>>() {
            @Override
            public Class<? extends JSONEvent> getEventType() {
                return JSONEvent.class;
            }
            
            @Override
            public Writer<JSONEvent, Tuple2<String,T>> writer() {
                return innerProtocol.writer()
                    .compose((Tuple2<String, T> t) -> t._2)
                    .mapWithInput((t, events) -> Vector.<JSONEvent>of(new JSONEvent.FieldName(t._1)).appendAll(events));
            }
            
            @Override
            public String toString() {
                return "(any): " + innerProtocol;
            }
        };
    }
}
