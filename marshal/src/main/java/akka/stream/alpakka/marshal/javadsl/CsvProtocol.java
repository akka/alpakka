package akka.stream.alpakka.marshal.javadsl;

import java.util.List;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.csv.CsvEvent;
import akka.stream.alpakka.marshal.csv.MultiReadProtocol;
import akka.stream.alpakka.marshal.csv.MultiWriteProtocol;
import akka.stream.alpakka.marshal.csv.NamedColumnReadProtocol;
import akka.stream.alpakka.marshal.csv.NamedColumnWriteProtocol;
import akka.stream.alpakka.marshal.csv.ValueProtocol;
import akka.stream.alpakka.marshal.generic.Locator;
import akka.stream.alpakka.marshal.generic.StringProtocol;
import javaslang.Function1;
import javaslang.Function2;
import javaslang.Function3;
import javaslang.collection.Vector;

/**
 * Contains static methods for the Java CSV marshalling DSL.
 */
@SuppressWarnings("unchecked")
public class CsvProtocol {
    private static final Locator<CsvEvent> locator = event -> "(unknown)"; // FIXME add location information to the CSV parser
    
    /**
     * Returns a read/write protocol for a CSV format in which only a single column is read or written.
     */
    public static StringProtocol<CsvEvent> column(String name) {
        return new StringProtocol<>(column(name, ValueProtocol.instance), locator);
    }
    
    //// ---- 2-ary methods -------------------------
    
    /**
     * Returns a read/write protocol for a CSV format consisting of several nested protocols (probably created
     * using {@link #column(String)} ), invoking the [produce] function on reading, and using the
     * specified getters for writing.
     */
    public static <T,F1,F2> Protocol<CsvEvent, T> csv(
        Protocol<CsvEvent,F1> p1, Protocol<CsvEvent,F2> p2,
        Function2<F1,F2,T> produce,
        Function1<T,F1> g1, Function1<T,F2> g2) {
        return multi(Vector.of(p1, p2), args -> produce.apply((F1) args.get(0), (F2) args.get(1)), Vector.of(g1, g2));
    }

    /**
     * Returns a read-only protocol for a CSV format consisting of several nested protocols (probably created
     * using {@link #column(String)} ), invoking the [produce] function on reading.
     */
    public static <T,F1,F2> ReadProtocol<CsvEvent, T> csv(
        ReadProtocol<CsvEvent,F1> p1,
        ReadProtocol<CsvEvent,F2> p2,
        Function2<F1,F2,T> produce) {
        return multi(Vector.of(p1, p2), args -> produce.apply((F1) args.get(0), (F2) args.get(1)));
    }

    /**
     * Returns a read/write protocol for a CSV format consisting of several nested protocols (probably created
     * using {@link #column(String)} ), using the
     * specified getters for writing.
     */
    public static <T,F1,F2> WriteProtocol<CsvEvent, T> csv(
        Function1<T,F1> g1, Protocol<CsvEvent,F1> p1,
        Function1<T,F2> g2, Protocol<CsvEvent,F2> p2) {
        return multi(Vector.of(p1, p2), Vector.of(g1, g2));
    }

    //// ---- 3-ary methods -------------------------
    
    /**
     * Returns a read/write protocol for a CSV format consisting of several nested protocols (probably created
     * using {@link #column(String)} ), invoking the [produce] function on reading, and using the
     * specified getters for writing.
     */
    public static <T,F1,F2,F3> Protocol<CsvEvent, T> csv(
        Protocol<CsvEvent,F1> p1, Protocol<CsvEvent,F2> p2, Protocol<CsvEvent,F3> p3,
        Function3<F1,F2,F3,T> produce,
        Function1<T,F1> g1, Function1<T,F2> g2, Function1<T,F3> g3) {
        return multi(Vector.of(p1, p2, p3), args -> produce.apply((F1) args.get(0), (F2) args.get(1), (F3) args.get(2)), Vector.of(g1, g2, g3));
    }

    /**
     * Returns a read-only protocol for a CSV format consisting of several nested protocols (probably created
     * using {@link #column(String)} ), invoking the [produce] function on reading.
     */
    public static <T,F1,F2,F3> ReadProtocol<CsvEvent, T> csv(
        ReadProtocol<CsvEvent,F1> p1,
        ReadProtocol<CsvEvent,F2> p2,
        ReadProtocol<CsvEvent,F3> p3,
        Function3<F1,F2,F3,T> produce) {
        return multi(Vector.of(p1, p2, p3), args -> produce.apply((F1) args.get(0), (F2) args.get(1), (F3) args.get(2)));
    }

    /**
     * Returns a read/write protocol for a CSV format consisting of several nested protocols (probably created
     * using {@link #column(String)} ), using the
     * specified getters for writing.
     */
    public static <T,F1,F2,F3> WriteProtocol<CsvEvent, T> csv(
        Function1<T,F1> g1, Protocol<CsvEvent,F1> p1,
        Function1<T,F2> g2, Protocol<CsvEvent,F2> p2,
        Function1<T,F3> g3, Protocol<CsvEvent,F3> p3) {
        return multi(Vector.of(p1, p2, p3), Vector.of(g1, g2, g3));
    }

    private static <T> Protocol<CsvEvent, T> multi(Vector<Protocol<CsvEvent, ?>> protocols, Function1<List<?>,T> produce, Vector<Function1<T, ?>> getters) {
        return Protocol.of(new MultiReadProtocol<>(Vector.narrow(protocols), produce), new MultiWriteProtocol<>(Vector.narrow(protocols), getters));
    }
    
    private static <T> ReadProtocol<CsvEvent, T> multi(Vector<ReadProtocol<CsvEvent, ?>> protocols, Function1<List<?>,T> produce) {
        return new MultiReadProtocol<>(Vector.narrow(protocols), produce);
    }
    
    private static <T> WriteProtocol<CsvEvent, T> multi(Vector<Protocol<CsvEvent, ?>> protocols, Vector<Function1<T, ?>> getters) {
        return new MultiWriteProtocol<>(Vector.narrow(protocols), getters);
    }
    
    private static <T> Protocol<CsvEvent, T> column(String name, Protocol<CsvEvent,T> inner) {
        return Protocol.of(new NamedColumnReadProtocol<>(name, inner), new NamedColumnWriteProtocol<>(name, inner));
    }
}
