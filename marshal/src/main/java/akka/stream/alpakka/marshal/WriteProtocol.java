package akka.stream.alpakka.marshal;

import javaslang.Function1;

/**
 * Protocol for writing possibly multiple instances of T into events E.
 */
public interface WriteProtocol<E,T> {
    /**
     * Narrows a protocol for writing T to become a writer for a subclass of T.
     * This is OK, since a writer that accepts a T for writing will also accept a U.
     */
    @SuppressWarnings("unchecked")
    public static <E, T, U extends T> WriteProtocol<E,U> narrow(WriteProtocol<E,T> w) {
        return (WriteProtocol<E,U>) w;
    }
    
    /**
     * Returns the specific subtype of events that this protocol will emit (if known, otherwise E)
     */
    public Class<? extends E> getEventType();
    
    /**
     * Creates and returns a new writer can write instances of T by emitting events of type E.
     */
    public abstract Writer<E,T> writer();

    /**
     * Maps the protocol into a different type, invoking [beforeWrite] before writing.
     */
    public default <U> WriteProtocol<E,U> compose(Function1<U,T> beforeWrite) {
        WriteProtocol<E,T> parent = this;
        return new WriteProtocol<E,U>() {
            @Override
            public Writer<E,U> writer() {
                return parent.writer().compose(beforeWrite);
            }
            
            @Override
            public Class<? extends E> getEventType() {
                return parent.getEventType();
            }
        };
    };
    
}
