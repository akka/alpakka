package akka.stream.alpakka.marshal.generic;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.Writer;
import javaslang.collection.Vector;

/**
 * Write support for generic Iterable, and read support for mutable collection classes.
 */
public class IterableProtocol {
    /**
     * @param factory Function that creates a new mutable collection with a single value
     * @param add Consumer that adds an element to the collection
     * @param empty Empty collection instance (global, it won't be modified)
     */
    public static <E,T,C extends Iterable<T>> ReadProtocol<E,C> read(ReadProtocol<E,T> inner, Supplier<C> factory, BiConsumer<C,T> add) {
        return FoldProtocol.read("*", inner, factory, (c,t) -> {
            add.accept(c, t);
            return c;
        });
    }
    
    public static <E,T> WriteProtocol<E,Iterable<T>> write(WriteProtocol<E,T> inner) {
        return new WriteProtocol<E,Iterable<T>>() {
            @Override
            public Writer<E,Iterable<T>> writer() {
                Writer<E,T> parentWriter = inner.writer();
                
                return Writer.of(iterable -> {
                    Vector<T> items = Vector.ofAll(iterable);
                    if (items.isEmpty()) {
                        return parentWriter.reset();
                    } else {
                        return items.map(parentWriter::applyAndReset)
                            .flatMap(Function.identity());
                    }
                });
            }
            
            @Override
            public Class<? extends E> getEventType() {
                return inner.getEventType();
            }
            
            @Override
            public String toString() {
                return "*(" + inner + ")";
            }
        };
    }
}
