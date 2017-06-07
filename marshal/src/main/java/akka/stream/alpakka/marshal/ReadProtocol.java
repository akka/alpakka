package akka.stream.alpakka.marshal;

import java.util.NoSuchElementException;

import javaslang.Function1;
import javaslang.control.Option;
import javaslang.control.Try;

/**
 * Protocol for reading a stream of events E into possibly multiple instances of T
 */
public interface ReadProtocol<E,T> {
    public static class Constants {
        private static final Try<?> NONE = Option.none().toTry();
    }
    
    /**
     * Widens a Reader for T to return a superclass S of T instead.
     * This is OK, since a ReadProtocol that produces T always produces a subclass of S.
     */
    @SuppressWarnings("unchecked")
    public static <E, S, T extends S> ReadProtocol<E,S> widen(ReadProtocol<E,T> p){
        return (ReadProtocol<E,S>) p;
    }
    

    /**
     * Returns a failed Try that indicates no value was found.
     */
    @SuppressWarnings("unchecked")
    public static <T> Try<T> none() {
        return (Try<T>) Constants.NONE;
    }

    /**
     * Checks whether the given Try indicates that no value was found (by being a failure of NoSuchElementException).
     */
    public static boolean isNone(Try<?> t) {
        return t.isFailure() && t.failed().get() instanceof NoSuchElementException;
    }
    
    public abstract Reader<E,T> reader();
    
    /**
     * Returns an appropriate representation of "empty" for this read protocol. By default, returns NONE, which is a failure.
     */
    public default Try<T> empty() {
        return none();
    }
    
    /**
     * Maps the protocol into a different type, invoking [onRead] after reading.
     */
    public default <U> ReadProtocol<E,U> map(Function1<T,U> onRead) {
        final ReadProtocol<E,T> parent = this;
        
        return new ReadProtocol<E,U>() {
            @Override
            public Reader<E, U> reader() {
                return parent.reader().map(onRead);
            }
            
            @Override
            public Try<U> empty() {
                return parent.empty().map(onRead);
            }
        };
    };
}
