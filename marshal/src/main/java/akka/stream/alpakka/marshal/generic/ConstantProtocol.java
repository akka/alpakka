package akka.stream.alpakka.marshal.generic;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.Writer;
import javaslang.control.Try;

/**
 * Protocol that always writes the same value, and on reading emits "Present" when the expected value was indeed read.
 */
public class ConstantProtocol {
    /**
     * Marker type to indicate that the constant was indeed found in the incoming event stream (during reading).
     * During writing, the constant is always output whenever the writer is invoked.
     */
    public static class Present {
        private Present() {}
        
        @Override
        public String toString() {
            return "present";
        }
    }
    public static final Present PRESENT = new Present();
    
    public static <E,T> ReadProtocol<E,Present> read(ReadProtocol<E,T> inner, T value) {
        return new ReadProtocol<E,Present>() {
            @Override
            public Reader<E,Present> reader() {
                final Reader<E,T> innerReader = inner.reader();
                
                return new Reader<E,Present>() {
                    @Override
                    public Try<Present> reset() {
                        return emit(innerReader.reset());
                    }

                    @Override
                    public Try<Present> apply(E evt) {
                        return emit(innerReader.apply(evt));
                    }
                    
                    private Try<Present> emit(Try<T> t) {
                        return t.filter(value::equals).map(v -> PRESENT);
                    }
                };
            }
            
            @Override
            public String toString() {
                return inner.toString() + "=" + value;
            }
        };
    }

    public static <E,T> WriteProtocol<E,Present> write(WriteProtocol<E,T> inner, T value) {
        return new WriteProtocol<E,Present>() {
            @Override
            public Writer<E,Present> writer() {
                return inner.writer().compose(present -> value);
            }

            @Override
            public Class<? extends E> getEventType() {
                return inner.getEventType();
            }
            
            @Override
            public String toString() {
                return inner.toString() + "=" + value;
            }
        };
    }

    public static <E,T> Protocol<E,Present> readWrite(Protocol<E,T> inner, T value) {
        return Protocol.of(read(inner, value), write(inner, value));
    }
}
