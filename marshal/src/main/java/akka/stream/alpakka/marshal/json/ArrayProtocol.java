package akka.stream.alpakka.marshal.json;

import static akka.stream.alpakka.marshal.ReadProtocol.isNone;
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

public class ArrayProtocol<E> {
    private static final Logger log = LoggerFactory.getLogger(ArrayProtocol.class);
    
    public static <E> ReadProtocol<JSONEvent, E> read(ReadProtocol<JSONEvent, E> innerProtocol) {
        return new ReadProtocol<JSONEvent, E>() {
            private final ReadProtocol<JSONEvent, E> owner = this;
            
            @Override
            public Reader<JSONEvent, E> reader() {
                return new Reader<JSONEvent, E>() {
                    private final Reader<JSONEvent, E> inner = innerProtocol.reader();
                    private int nestedObjects = 0;
                    private boolean matched = false;
                    private boolean wasEmpty = true;

                    @Override
                    public Try<E> reset() {
                        nestedObjects = 0;
                        matched = false;
                        wasEmpty = true;
                        return none();
                    }

                    @Override
                    public Try<E> apply(JSONEvent evt) {
                        if (nestedObjects == 0) {
                            if (evt == JSONEvent.START_OBJECT) {
                                nestedObjects++;
                                return none();
                            } else if (evt == JSONEvent.START_ARRAY) {
                                log.debug("Array has started: {}", owner);
                                matched = true;
                                nestedObjects++;
                                return none();
                            } else { // literal, just skip
                                return none();
                            }
                        } else if (matched && evt == JSONEvent.END_ARRAY && nestedObjects == 1) {
                            log.debug("Array has ended: {}", owner);
                            reset();
                            Try<E> result = inner.reset();
                            return (wasEmpty && isNone(result)) ? innerProtocol.empty() : result;
                        } else {
                            Try<E> result = none();
                            
                            if (matched) {
                                log.debug("Array forwarding {} at level {}", evt, nestedObjects);
                                wasEmpty = false;
                                result = inner.apply(evt);
                            }
                            
                            if (evt == JSONEvent.END_ARRAY || evt == JSONEvent.END_OBJECT) {
                                log.debug("    (nested--) {} on {}", nestedObjects, owner);
                                nestedObjects--;
                            } else if (evt == JSONEvent.START_ARRAY || evt == JSONEvent.START_OBJECT) {
                                log.debug("    (nested++) {} on {}", nestedObjects, owner);
                                nestedObjects++;
                            }
                            
                            return result;
                        }
                    }
                };
            }
            
            @Override
            public Try<E> empty() {
                return innerProtocol.empty();
            }
            
            @Override
            public String toString() {
                return "[ " + innerProtocol + "]";
            }
        };
    }

    public static <E> WriteProtocol<JSONEvent, E> write(WriteProtocol<JSONEvent, E> innerProtocol) {
        return new WriteProtocol<JSONEvent, E>() {
            WriteProtocol<JSONEvent, E> parent = this;
            
            @Override
            public Class<? extends JSONEvent> getEventType() {
                return JSONEvent.class;
            }
            
            @Override
            public Writer<JSONEvent, E> writer() {
                Writer<JSONEvent, E> inner = innerProtocol.writer();
                
                return new Writer<JSONEvent,E>() {
                    boolean started = false;

                    @Override
                    public Seq<JSONEvent> apply(E value) {
                        log.debug("{}: Writing {}, started {}", parent, value, started);
                        Seq<JSONEvent> prefix = (started) ? Vector.empty() : Vector.of(JSONEvent.START_ARRAY);
                        started = true;
                        
                        return prefix.appendAll(inner.applyAndReset(value));
                    }
                        
                    @Override
                    public Seq<JSONEvent> reset() {
                        log.debug("{}: Resetting ", parent);
                        Seq<JSONEvent> prefix = (started) ? Vector.empty() : Vector.of(JSONEvent.START_ARRAY);
                        started = false;
                        
                        return prefix.append(JSONEvent.END_ARRAY);
                    }
                };
            }
            
            @Override
            public String toString() {
                return "[ " + innerProtocol + "]";
            }
        };
    }
}
