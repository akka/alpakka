package akka.stream.alpakka.marshal;

import javaslang.Function1;
import javaslang.control.Try;

public interface Reader<E,T> {
    /**
     * A Reader that produces U always produces a subclass of T.
     */
    @SuppressWarnings("unchecked")
    public static <E, T, U extends T> Reader<E,T> narrow(Reader<E,U> p){
        return (Reader<E,T>) p;
    }
    
    /**
     * Returns a reader that emits a result on every event, the result being the event itself.
     */
    public static <T, E extends T> Reader<E,T> identity() {
        return new Reader<E, T>() {
            @Override
            public Try<T> reset() {
                return ReadProtocol.none();
            }

            @Override
            public Try<T> apply(E event) {
                return Try.success(event);
            }
        };
    }
    
    Try<T> reset();
    Try<T> apply(E event);
    
    public default <U> Reader<E,U> map(Function1<T,U> f) {
        Reader<E,T> parent = this;
        return new Reader<E,U>() {
            @Override
            public Try<U> reset() {
                return parent.reset().mapTry(f::apply);
            }

            @Override
            public Try<U> apply(E evt) {
                return parent.apply(evt).mapTry(f::apply);
            }
        };
    }
    
    public default <U> Reader<E,U> flatMap(Function1<T,Try<U>> f) {
        Reader<E,T> parent = this;
        return new Reader<E,U>() {
            @Override
            public Try<U> reset() {
                return parent.reset().flatMap(f);
            }

            @Override
            public Try<U> apply(E evt) {
                return parent.apply(evt).flatMap(f);
            }
        };
    }
    
}
