package akka.stream.alpakka.marshal.generic;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;
import javaslang.control.Try;

/**
 * A protocol for a String that has been marshalled to and from T using a {@link StringMarshallable}.
 */
public class StringMarshallableProtocol<E,T> implements Protocol<E,T> {
    private final Locator<E> locator;
    private final StringMarshallable<T> type;
    private final Protocol<E,String> delegate;
    
    public StringMarshallableProtocol(StringMarshallable<T> type, Protocol<E,String> delegate, Locator<E> locator) {
        this.type = type;
        this.delegate = delegate;
        this.locator = locator;
    }

    @Override
    public Writer<E,T> writer() {
        return delegate.writer().compose(type::write);
    }

    @Override
    public Reader<E,T> reader() {
        return addLocationOnError(delegate.reader().flatMap(s -> type.tryRead(s)), locator);
    }

    @Override
    public Class<? extends E> getEventType() {
        return delegate.getEventType();
    }
    
    @Override
    public String toString() {
        return delegate.toString() + " as " + type.toString();
    }

    static <E,T> Reader<E,T> addLocationOnError(Reader<E,T> inner, Locator<E> locator) {
        return new Reader<E,T>() {
            @Override
            public Try<T> reset() {
                return inner.reset();
            }

            @Override
            public Try<T> apply(E event) {
                return addLocation(inner.apply(event), locator.getLocation(event));
            }
            
            private Try<T> addLocation(Try<T> t, String location) {
                if (t.isFailure() && t.failed().get() instanceof IllegalArgumentException) {
                    return Try.failure(new IllegalArgumentException(t.failed().get().getMessage() + " at " + location, t.failed().get()));
                } else {
                    return t;
                }
            }
        };
    }
}
