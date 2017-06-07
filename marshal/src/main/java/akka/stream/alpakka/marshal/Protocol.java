package akka.stream.alpakka.marshal;

import javaslang.Function1;
import javaslang.control.Try;

public interface Protocol<E,T> extends ReadProtocol<E,T>, WriteProtocol<E,T> {
    /**
     * Returns a protocol that considers all events emitted results, and vice-versa.
     */
    public static <T> Protocol<T,T> identity(Class<T> type) {
        return new Protocol<T,T>() {
            @Override
            public Reader<T, T> reader() {
                return Reader.identity();
            }

            @Override
            public Class<? extends T> getEventType() {
                return type;
            }

            @Override
            public Writer<T, T> writer() {
                return Writer.identity();
            }
        };
    }
    
    /**
     * Combines a read-only and a write-only protocol into a read/write protocol
     */
    public static <E,T> Protocol<E,T> of(ReadProtocol<E,T> read, WriteProtocol<E,T> write) {
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
            
            @Override
            public Try<T> empty() {
                return read.empty();
            }
            
            @Override
            public String toString() {
                return read.toString();
            }
        };
    }
    
    /**
     * Maps the protocol into a different type, invoking [onRead] after reading and [beforeWrite] before writing.
     */
    public default <U> Protocol<E,U> map(Function1<T,U> onRead, Function1<U,T> beforeWrite) {
        Protocol<E,T> parent = this;
        return new Protocol<E,U>() {
            @Override
            public Reader<E,U> reader() {
                return parent.reader().map(onRead);
            }

            @Override
            public Writer<E,U> writer() {
                return parent.writer().compose(beforeWrite);
            }
            
            @Override
            public Try<U> empty() {
                return parent.empty().map(onRead);
            }
            
            @Override
            public Class<? extends E> getEventType() {
                return parent.getEventType();
            }
        };
    };
}
