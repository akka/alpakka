package akka.stream.alpakka.marshal.csv;

import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.Writer;

import javaslang.collection.Seq;
import javaslang.collection.Vector;

/**
 * Writes the column name + endValue() + endRecord() on the first line,
 * and routes to to the inner protocol for all values found within subsequent lines.
 * 
 * The inner protocol is expected to only emit {@link CsvEvent.Text} events.
 */
public class NamedColumnWriteProtocol<T> implements WriteProtocol<CsvEvent, T> {
    private final String name;
    private final WriteProtocol<CsvEvent, T> inner;
    
    public NamedColumnWriteProtocol(String name, WriteProtocol<CsvEvent, T> inner) {
        this.name = name;
        this.inner = inner;
    }

    @Override
    public Class<? extends CsvEvent> getEventType() {
        return inner.getEventType();
    }

    @Override
    public Writer<CsvEvent, T> writer() {
        Writer<CsvEvent, T> innerWriter = inner.writer();
        
        return new Writer<CsvEvent, T>() {
            boolean first = true;
            
            @Override
            public Seq<CsvEvent> apply(T value) {
                Seq<CsvEvent> events =
                    (first) ? Vector.of(CsvEvent.text(name), CsvEvent.endValue(), CsvEvent.endRecord()) : Vector.empty();
                first = false;
                return events.appendAll(innerWriter.apply(value)).append(CsvEvent.endValue()).append(CsvEvent.endRecord());
            }

            @Override
            public Seq<CsvEvent> reset() {
                if (first) {
                    first = false;
                    return Vector.of(CsvEvent.text(name), CsvEvent.endValue(), CsvEvent.endRecord());
                } else {
                    return Vector.empty();
                }
            }
        };
    }
    
}
