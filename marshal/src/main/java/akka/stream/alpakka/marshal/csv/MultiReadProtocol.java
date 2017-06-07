package akka.stream.alpakka.marshal.csv;

import static akka.stream.alpakka.marshal.ReadProtocol.isNone;
import static akka.stream.alpakka.marshal.ReadProtocol.none;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;

import javaslang.Function1;
import javaslang.collection.Seq;
import javaslang.collection.Vector;
import javaslang.control.Try;

/**
 * Combines several nested protocols that each return separate values into a single value,
 * that is emitted at the end of every record.
 */
public class MultiReadProtocol<T> implements ReadProtocol<CsvEvent, T> {
    private static final Logger log = LoggerFactory.getLogger(MultiReadProtocol.class);
    
    private final Seq<ReadProtocol<CsvEvent,?>> protocols;
    private final Function1<List<?>, T> produce;

    /**
     * Creates a MultiReadProtocol
     * @param protocols The nested protocols to forward all events to
     * @param produce Function that is invoked with a list of what the nested protocols have emitted, at the
     * end of every record.
     */
    public MultiReadProtocol(Iterable<ReadProtocol<CsvEvent,?>> protocols, Function1<List<?>, T> produce) {
        this.protocols = Vector.ofAll(protocols);
        this.produce = produce;
    }
    
    @Override
    public Reader<CsvEvent, T> reader() {
        @SuppressWarnings("unchecked")
        Seq<Reader<CsvEvent, Object>> readers = protocols.map(c -> (Reader<CsvEvent,Object>) c.reader());
        @SuppressWarnings("unchecked")
        Try<Object>[] values = new Try[protocols.size()];
        
        return new Reader<CsvEvent, T>() {
            {
                reset();
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public Try<T> reset() {
                readers.forEach(r -> r.reset());
                Arrays.fill(values, none());
                for (int i = 0; i < protocols.size(); i++) {
                    values[i] = (Try<Object>) protocols.get(i).empty();
                }
                return none();
            }

            @Override
            public Try<T> apply(CsvEvent event) {
                for (int i = 0; i < protocols.size(); i++) {
                    Try<Object> result = readers.apply(i).apply(event);
                    if (!isNone(result)) {
                        values[i] = result;
                    }
                }
                
                if (event instanceof CsvEvent.EndRecord) {
                    // end of line -> time to emit a value
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    for (int i = 0; i < readers.size(); i++) {
                        Try<Object> r = readers.get(i).reset();
                        log.debug("{} reset: {}", protocols.get(i), r);
                        if (!isNone(r) && values[i].eq(protocols.get(i).empty())) {
                            values[i] = r;
                        }
                    }
                    Object[] args = new Object[protocols.size()];
                    for (int i = 0; i < values.length; i++) {
                        Try<Object> t = values[i];
                        t.failed().forEach(failure::set);
                        args[i] = t.getOrElse((Object)null);
                    }
                    Try<T> result = (failure.get() != null) ? Try.failure(failure.get()) : Try.success(produce.apply(Arrays.asList(args)));
                    reset();
                    log.debug("{} emitting {}", MultiReadProtocol.this, result);
                    return result;
                } else {
                    return none();
                }
            }
            
        };
    }
    
    @Override
    public String toString() {
        return "(" + protocols.mkString("; ") + ")";
    }
}
