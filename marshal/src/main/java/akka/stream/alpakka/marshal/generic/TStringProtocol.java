package akka.stream.alpakka.marshal.generic;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.control.Try;

/**
 * Protocol for a tuple of T and String, where the String can be transformed to other types
 * by invoking .as(TYPE).
 */
public class TStringProtocol<E,T1> implements Protocol<E,Tuple2<T1, String>> {
    private final Protocol<E,Tuple2<T1, String>> delegate;
    private final Locator<E> locator;
    
    public TStringProtocol(Protocol<E,Tuple2<T1, String>> delegate, Locator<E> locator) {
        this.delegate = delegate;
        this.locator = locator;
    }
    
    @Override
    public  Class<? extends E> getEventType() {
        return delegate.getEventType();
    }

    @Override
    public Reader<E, Tuple2<T1, String>> reader() {
        return delegate.reader();
    }

    @Override
    public Try<Tuple2<T1, String>> empty() {
        return delegate.empty();
    }

    @Override
    public Writer<E, Tuple2<T1, String>> writer() {
        return delegate.writer();
    }
    
    @Override
    public String toString() {
        return delegate.toString();
    }

    /**
     * Converts _2 of the tuple to a different type.
     */
    public <T2> Protocol<E,Tuple2<T1,T2>> as(StringMarshallable<T2> type) {
        return new Protocol<E,Tuple2<T1,T2>>() {
            @Override
            public Writer<E,Tuple2<T1, T2>> writer() {
                return TStringProtocol.this.writer().compose(t -> t.map2(type::write));
            }

            @Override
            public Try<Tuple2<T1, T2>> empty() {
                return TStringProtocol.this.empty().flatMap(tuple -> 
                    type.tryRead(tuple._2()).map(t2 -> Tuple.of(tuple._1, t2)));
            }
            
            @Override
            public Reader<E,Tuple2<T1, T2>> reader() {
                return StringMarshallableProtocol.addLocationOnError(
                    TStringProtocol.this.reader().flatMap(tuple -> 
                        type.tryRead(tuple._2()).map(t2 -> Tuple.of(tuple._1(), t2)))
                    , locator);
            }
            
            @Override
            public Class<? extends E> getEventType() {
                return delegate.getEventType();
            }
            
            @Override
            public String toString() {
                return delegate.toString() + " as " + type.toString();
            }            
        };
    }    
    
}
