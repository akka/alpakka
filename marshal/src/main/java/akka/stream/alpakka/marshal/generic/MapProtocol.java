package akka.stream.alpakka.marshal.generic;

import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.WriteProtocol;
import javaslang.Function1;
import javaslang.Tuple2;
import javaslang.collection.Map;

/**
 * Reads and writes an inner read/write protocol of tuples, which may match multiple times, 
 * by mapping it to an immutable {@link javaslang.collection.Map}.
 */
public class MapProtocol {
    /**
     * Reads a series of tuples into a map.
     * @param factory Function that creates a singleton collection around a value
     * @param empty Empty collection
     */
    @SuppressWarnings("unchecked")
    public static <E,K,V,M extends Map<K,V>> ReadProtocol<E,M> read(ReadProtocol<E,Tuple2<K,V>> inner, Function1<Tuple2<K,V>,M> factory, M empty) {
        return FoldProtocol.read("map", inner, () -> empty, (map, t) -> (M) map.put(t));
    }

    /**
     * Writes a map by considering it a series of tuples.
     */
    public static <E,K,V> WriteProtocol<E,Map<K,V>> write(WriteProtocol<E,Tuple2<K,V>> inner) {
        return WriteProtocol.narrow(IterableProtocol.write(inner));
    }    
}
