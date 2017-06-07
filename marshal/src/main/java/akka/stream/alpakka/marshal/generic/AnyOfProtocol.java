package akka.stream.alpakka.marshal.generic;

import static akka.stream.alpakka.marshal.ReadProtocol.isNone;
import static akka.stream.alpakka.marshal.ReadProtocol.none;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;
import javaslang.collection.Seq;
import javaslang.control.Try;

/**
 * Forwards read events to multiple alternative protocols, emitting whenever any of the alternatives emit. If multiple
 * alternatives emit for the same event, the first one wins.
 */
public class AnyOfProtocol<E,T> implements ReadProtocol<E,T> {
    private static final Logger log = LoggerFactory.getLogger(AnyOfProtocol.class);
    
    private final Seq<ReadProtocol<E,T>> alternatives;

    public AnyOfProtocol(Seq<ReadProtocol<E,T>> alternatives) {
        this.alternatives = alternatives;
    }

    @Override
    public Reader<E,T> reader() {
        Seq<Reader<E,T>> readers = alternatives.map(p -> p.reader());
        return new Reader<E,T>() {
            @Override
            public Try<T> reset() {
                return perform(r -> r.reset());
            }

            @Override
            public Try<T> apply(E evt) {
                return perform(r -> r.apply(evt));
            }
            
            private Try<T> perform(Function<Reader<E,T>, Try<T>> f) {
                Try<T> result = none();
                for (Reader<E,T> reader: readers) {
                    Try<T> readerResult = f.apply(reader);
                    log.debug("reader {} said {}", reader, readerResult);
                    if (!isNone(readerResult)) {
                        if (isNone(result) || (result.isFailure() && readerResult.isSuccess())) {
                            result = readerResult;
                        } else if (readerResult.isFailure() && result.isFailure()) {
                            result = Try.failure(new IllegalArgumentException(result.failed().get().getMessage() + ", alternatively " + readerResult.failed().get().getMessage()));
                        }
                    }
                }
                return result;
            }
        };
    }
    
    /**
     * Returns a Protocol that uses an AlternativesProtocol for reading, and always picks the first alternative when writing.
     */
    public static <E,T> Protocol<E,T> readWrite(Seq<Protocol<E,T>> alternatives) {
        Protocol<E,T> write = alternatives.head();
        AnyOfProtocol<E,T> read = new AnyOfProtocol<>(Seq.narrow(alternatives));
        return new Protocol<E,T>() {
            @Override
            public Writer<E,T> writer() {
                return write.writer();
            }

            @Override
            public Class<? extends E> getEventType() {
                return write.getEventType();
            }

            @Override
            public Reader<E,T> reader() {
                return read.reader();
            }
        };
    }
}
