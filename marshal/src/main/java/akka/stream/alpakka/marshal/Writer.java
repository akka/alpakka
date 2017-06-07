package akka.stream.alpakka.marshal;

import java.util.function.BiFunction;
import java.util.function.Function;

import javaslang.Function1;
import javaslang.collection.Seq;
import javaslang.collection.Vector;

/**
 * Writes out instances of T into sequences of events E. Implementations of this interface
 * are not safe to be called from multiple threads simultaneously.
 */
public interface Writer<E,T> {
    /**
     * Widens a writer of U to become a writer of a superclass of U. This is OK, since a writer that
     * produces U is in fact producing a T.
     */
    @SuppressWarnings("unchecked")
    public static <E, T, U extends T> Writer<E,T> widen(Writer<E,U> w) {
        return (Writer<E,T>) w;
    }
    
    /**
     * Returns a Writer that calls the given function for apply(), and nothing for reset().
     */
    public static <E,T> Writer<E,T> of (Function<T,Seq<E>> f) {
        return new Writer<E, T>() {
            @Override
            public Seq<E> apply(T value) {
                return f.apply(value);
            }

            @Override
            public Seq<E> reset() {
                return Vector.empty();
            }
        };
    }
    
    /**
     * Returns a writer that emits an event for every element, the event being the element itself.
     */
    public static <E, T extends E> Writer<E,T> identity() {
        return new Writer<E,T>() {
            @Override
            public Seq<E> apply(T value) {
                return Vector.of(value);
            }
            
            @Override
            public Seq<E> reset() {
                return Vector.empty();
            }
        };
    }
    
    /**
     * Writes out the given value, and resets the writer.
     */
    public default Seq<E> applyAndReset(T value) {
        return apply(value).appendAll(reset());
    }
    
    /**
     * Writes out a value, and returns the events to be emitted for that value. Can be called repeatedly.
     */
    Seq<E> apply(T value);
    
    /**
     * Signals completion of the write process, returning any final events to emit.
     * The writer is reset to an initial state after this.
     */
    Seq<E> reset();

    /**
     * Invokes the given function before calling this writer.
     */
    public default <U> Writer<E,U> compose(Function1<U,T> f) {
        Writer<E,T> parent = this;
        return new Writer<E,U>() {
            @Override
            public Seq<E> apply(U value) {
                return parent.apply(f.apply(value));
            }
            
            @Override
            public Seq<E> reset() {
                return parent.reset();
            }
        };
    }
    
    /**
     * Applies the given transformation to the returned events of {@link #apply} and {@link #reset}
     */
    public default Writer<E,T> map(Function<Seq<E>, Seq<E>> f) {
        Writer<E,T> parent = this;
        return new Writer<E,T>() {
            @Override
            public Seq<E> apply(T value) {
                return f.apply(parent.apply(value));
            }

            @Override
            public Seq<E> reset() {
                return f.apply(parent.reset());
            }
        };
    }
    
    /**
     * Applies the given transformation to the returned events of {@link #apply} (but not to {@link #reset}), also passing in the original input
     */
    public default Writer<E,T> mapWithInput(BiFunction<T, Seq<E>, Seq<E>> f) {
        Writer<E,T> parent = this;
        return new Writer<E,T>() {
            @Override
            public Seq<E> apply(T value) {
                return f.apply(value, parent.apply(value));
            }

            @Override
            public Seq<E> reset() {
                return parent.reset();
            }
        };
    }
    
}
