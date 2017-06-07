package akka.stream.alpakka.marshal.generic;

import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.WriteProtocol;
import javaslang.collection.Seq; 

public class SeqProtocol {
    
    @SuppressWarnings("unchecked")
    public static <E,T,S extends Seq<T>> ReadProtocol<E, S> read(ReadProtocol<E, T> inner, S empty) {
        return FoldProtocol.read("*", inner, () -> empty, (seq, t) -> (S) seq.append(t));
    }

    public static <E,T> WriteProtocol<E,Seq<T>> write(WriteProtocol<E,T> inner) {
        return WriteProtocol.narrow(IterableProtocol.write(inner));
    }
    
}
