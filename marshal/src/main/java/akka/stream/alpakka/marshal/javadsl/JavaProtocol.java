package akka.stream.alpakka.marshal.javadsl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.generic.AnyOfProtocol;
import akka.stream.alpakka.marshal.generic.CombinedProtocol;
import akka.stream.alpakka.marshal.generic.FoldProtocol;
import akka.stream.alpakka.marshal.generic.IterableProtocol;
import akka.stream.alpakka.marshal.generic.MapProtocol;
import akka.stream.alpakka.marshal.generic.OptionProtocol;
import akka.stream.alpakka.marshal.generic.SeqProtocol;
import javaslang.Function2;
import javaslang.Tuple2;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Seq;
import javaslang.collection.Vector;
import javaslang.control.Option;

/**
 * Contains Java DSL protocol factory methods
 */
public class JavaProtocol {
    // ----------------------- Alternatives -----------------------------
    
    /**
     * Forwards read events to multiple alternative protocols, emitting whenever any of the alternatives emit. If multiple
     * alternatives emit for the same event, the first one wins.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E,T> ReadProtocol<E,T> anyOf(ReadProtocol<E,T> first, ReadProtocol<E,T> second, ReadProtocol<E,T>... others) {
        return new AnyOfProtocol<>(Vector.of(first, second).appendAll(Arrays.asList(others)));
    }

    /**
     * Forwards read events to multiple alternative protocols, emitting whenever any of the alternatives emit. If multiple
     * alternatives emit for the same event, the first one wins.
     * 
     * Always picks the first alternative during writing.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E,T> Protocol<E,T> anyOf(Protocol<E,T> first, Protocol<E,T> second, Protocol<E,T>... others) {
        return AnyOfProtocol.readWrite(Vector.of(first, second).appendAll(Arrays.asList(others)));
    }

    /**
     * Forwards read events to multiple alternative protocols, emitting whenever any of the alternatives emit.
     * If multiple alternatives emit for the same event, all results are emitted.
     * If at least one alternative emits for an event, any errors on other alternatives are ignored.
     * If all alternatives yield errors for an event, the errors are concatenated and escalated.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E,T> ReadProtocol<E,Seq<T>> combine(ReadProtocol<E,T> first, ReadProtocol<E,T> second, ReadProtocol<E,T>... others) {
        return new CombinedProtocol<>(Vector.of(first, second).appendAll(Arrays.asList(others)));
    }
    
    // ----------------------- Collections -----------------------------
    
    /**
     * Reads an inner protocol multiple times. On reading, creates a {@link javaslang.collection.Vector} to represent it.
     */
    public static <E,T> ReadProtocol<E,Vector<T>> vector(ReadProtocol<E,T> inner) {
        return SeqProtocol.read(inner, Vector.empty());
    }
    
    /**
     * Reads and writes an inner protocol multiple times. On reading, creates a {@link javaslang.collection.Vector} to hold the values.
     * On writing, any {@link javaslang.collection.Seq} will do.
     */
    public static <E,T> Protocol<E,Seq<T>> vector(Protocol<E,T> inner) {
        return Protocol.of(SeqProtocol.read(inner, Vector.empty()), SeqProtocol.write(inner));
    }

    /**
     * Writes an inner protocol multiple times, represented by a {@link javaslang.collection.Seq}.
     */
    public static <E,T> WriteProtocol<E,Seq<T>> seq(WriteProtocol<E,T> inner) {
        return SeqProtocol.write(inner);
    }

    /**
     * Folds over a repeated nested protocol, merging the results into a single element.
     */
    public static <E,T,U> ReadProtocol<E,U> foldLeft(ReadProtocol<E,T> inner, Supplier<U> initial, Function2<U,T,U> combine) {
        return FoldProtocol.read("*", inner, initial, combine);
    }
    
    /**
     * Invokes the given function for every item the inner protocol emits, while emitting a single null as outer value.
     */
    public static <E,T> ReadProtocol<E,Void> forEach(ReadProtocol<E,T> inner, Consumer<T> consumer) {
        return FoldProtocol.read("*", inner, () -> null, (v1,v2) -> { consumer.accept(v2); return null; });
    }
    
    /**
     * Writes an inner protocol multiple times, represented by a {@link java.util.Iterable}.
     */
    public static <E,T> WriteProtocol<E,Iterable<T>> iterable(WriteProtocol<E,T> inner) {
        return IterableProtocol.write(inner);
    }

    /**
     * Reads an inner protocol multiple times. On reading, creates a {@link java.util.ArrayList} to represent it.
     */
    public static <E,T> ReadProtocol<E,ArrayList<T>> arrayList(ReadProtocol<E,T> inner) {
        return IterableProtocol.read(inner, () -> new ArrayList<>(), java.util.List::add);
    }
    
    /**
     * Reads and writes an inner protocol multiple times. On reading, creates a {@link java.util.ArrayList} to hold the values.
     * On writing, any {@link java.util.List} will do.
     */
    public static <E,T> Protocol<E,java.util.List<T>> arrayList(Protocol<E,T> inner) {
        return Protocol.of(ReadProtocol.widen(arrayList((ReadProtocol<E,T>)inner)), WriteProtocol.narrow(iterable(inner)));
    }
    
    /**
     * Reads and writes an inner protocol of tuples multiple times. On reading, creates a {@link javaslang.collection.HashMap} to hold the result.
     * On writing, any {@link javaslang.collection.Map} will do.
     */
    public static <E,K,V> Protocol<E,Map<K,V>> hashMap(Protocol<E,Tuple2<K,V>> inner) {
        return Protocol.of(ReadProtocol.widen(hashMap((ReadProtocol<E,Tuple2<K,V>>) inner)), map(inner));
    }
    
    /**
     * Reads an inner protocol of tuples multiple times. On reading, creates a {@link javaslang.collection.HashMap} to hold the result.
     */
    public static <E,K,V> ReadProtocol<E,HashMap<K,V>> hashMap(ReadProtocol<E,Tuple2<K,V>> inner) {
        return MapProtocol.read(inner, HashMap::of, HashMap.empty());
    }
    
    /**
     * Writes a map using an inner protocol, by turning it into writing multiple tuples.
     */
    public static <E,K,V> WriteProtocol<E,Map<K,V>> map(Protocol<E,Tuple2<K,V>> inner) {
        return MapProtocol.write(inner);
    }
    
    /**
     * Reads and writes a nested protocol optionally, representing it by a {@link javaslang.control.Option}.
     */
    public static <E,T> Protocol<E,Option<T>> option(Protocol<E,T> inner) {
        return Protocol.of(OptionProtocol.read(inner), OptionProtocol.write(inner));
    }
    
    /**
     * Reads a nested protocol optionally, representing it by a {@link javaslang.control.Option}.
     */
    public static <E,T> ReadProtocol<E,Option<T>> option(ReadProtocol<E,T> inner) {
        return OptionProtocol.read(inner);
    }

    /**
     * Writes a nested protocol optionally, representing it by a {@link javaslang.control.Option}.
     */
    public static <E,T> WriteProtocol<E,Option<T>> option(WriteProtocol<E,T> inner) {
        return OptionProtocol.write(inner);
    }
}
