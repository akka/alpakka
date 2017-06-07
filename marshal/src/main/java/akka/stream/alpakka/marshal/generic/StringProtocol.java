package akka.stream.alpakka.marshal.generic;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;
import javaslang.Function1;
import javaslang.control.Try;

/**
 * Protocol for String, which can be converted to other types using .as(), or made to conform
 * to a regular expression using .matching().
 */
public class StringProtocol<E> implements Protocol<E,String> {
    private final Locator<E> locator;
    private final Protocol<E,String> delegate;
    
    public StringProtocol(Protocol<E, String> delegate, Locator<E> locator) {
        this.delegate = delegate;
        this.locator = locator;
    }
    
    @Override
    public Writer<E,String> writer() {
        return delegate.writer();
    }

    @Override
    public Reader<E,String> reader() {
        return delegate.reader();
    }

    @Override
    public Class<? extends E> getEventType() {
        return delegate.getEventType();
    }
    
    @Override
    public String toString() {
        return delegate.toString();
    }
    
    /**
     * Converts the body to a different type.
     */
    public <T> Protocol<E,T> as(StringMarshallable<T> type) {
        return new StringMarshallableProtocol<>(type, this, locator);
    }
    
    /**
     * Returns an XMLProtocol that only reads strings that match the given regular expression. The resulting
     * protocol can not be used for writing.
     */
    public <T> ReadProtocol<E,T> matching(Regex<T> regex) {
        StringProtocol<E> parent = this;
        return new ReadProtocol<E,T>() {
            @Override
            public Reader<E,T> reader() {
                return parent.reader().flatMap(s -> regex.match(s).toTry());
            }
        };
    }

    /**
     * Returns an XMLProtocol that only reads strings that match the given regular expression.
     * During writing, the given function is called. It's the callers responsibility to ensure that
     * the result of the function matches the regex.
     */
    public <T> Protocol<E,T> matching(Regex<T> regex, Function1<T,String> onWrite) {
        StringProtocol<E> parent = this;
        return new Protocol<E,T>() {
            @Override
            public Reader<E,T> reader() {
                return parent.reader().flatMap(s -> regex.match(s).toTry().orElse(() -> Try.failure(new ValidationException(parent.toString() + " should match " + regex))));
            }

            @Override
            public Writer<E,T> writer() {
                return parent.writer().compose(onWrite);
            }
            
            @Override
            public Class<? extends E> getEventType() {
                return parent.getEventType();
            }
            
            @Override
            public String toString() {
                return parent.toString() + " matching " + regex;
            }
        };
    }

}
