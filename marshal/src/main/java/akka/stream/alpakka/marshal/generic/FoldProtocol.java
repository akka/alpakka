package akka.stream.alpakka.marshal.generic;

import static akka.stream.alpakka.marshal.ReadProtocol.isNone;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import javaslang.Function2;
import javaslang.control.Option;
import javaslang.control.Try;

/**
 * Folds over a repeated nested protocol, merging the results into a single element. Only for read protocols.
 */
public class FoldProtocol {
    private static final Logger log = LoggerFactory.getLogger(FoldProtocol.class);

    /**
     * Returns a protocol that accumulates multiple read emissions from an inner protocol into a single value.
     * @param name Name for the operation (appears in toString)
     * @param inner Inner protocol that emits the elements
     * @param initial Initial value to start accumulating on
     * @param combine Function that combines the previous result (or initial) with a parsed element
     */
    public static <E,T,U> ReadProtocol<E,U> read(String name, ReadProtocol<E,T> inner, Supplier<U> initial, Function2<U,T,U> combine) {
        Try<U> EMPTY = Try.success(initial.get());
        
        return new ReadProtocol<E,U>() {
            ReadProtocol<E,U> parent = this;
            @Override
            public Reader<E,U> reader() {
                Reader<E,T> parentReader = inner.reader();
                return new Reader<E,U>() {
                    Option<U> value = Option.none();
                    
                    @Override
                    public Try<U> apply(E evt) {
                        Try<T> result = parentReader.apply(evt);
                        if (result.isSuccess()) {
                            value = Option.some(combine.apply(value.getOrElse(initial), result.get()));
                            log.debug("Success, now {}", value);
                            return ReadProtocol.none();
                        } else if (isNone(result)) {
                            // skip over
                            return ReadProtocol.none();
                        } else {
                            // failure: emit immediately, throw away rest
                            log.debug("Failure: {}", result);
                            value = Option.none();
                            return result.map(t->null);
                        }
                    }

                    @Override
                    public Try<U> reset() {
                        Try<T> result = parentReader.reset();
                        Try<U> r;
                        if (result.isSuccess()) {
                            r = Try.success(combine.apply(value.getOrElse(initial), result.get()));
                        } else if (isNone(result)) {
                            r = (value.isDefined()) ? value.toTry() : EMPTY;
                        } else {
                            r = Try.failure(result.failed().get());
                        }
                        value = Option.none();
                        log.debug("{} reset as {}", parent, r);
                        return r;
                    }
                };
            }
            
            @Override
            public Try<U> empty() {
                return EMPTY;
            }
            
            @Override
            public String toString() {
                return name + "(" + inner.toString() + ")";
            }
        };
    }
}
