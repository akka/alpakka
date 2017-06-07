package akka.stream.alpakka.marshal.csv;

import static akka.stream.alpakka.marshal.ReadProtocol.none;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;

import javaslang.collection.Seq;
import javaslang.collection.Vector;
import javaslang.control.Try;

/**
 * Aggregates CSV text events into a String when reading, and outputs a single String as a single CSV text event when writing.
 */
public class ValueProtocol implements Protocol<CsvEvent,String> {
    public static final ValueProtocol instance = new ValueProtocol();
    
    @Override
    public Reader<CsvEvent, String> reader() {
        return new Reader<CsvEvent, String>() {
            private StringBuilder buffer = new StringBuilder();
            
            @Override
            public Try<String> reset() {
                Try<String> result = (buffer.length() > 0) ? Try.success(buffer.toString()) : none();
                buffer = new StringBuilder();
                return result;
            }

            @Override
            public Try<String> apply(CsvEvent event) {
                if (event instanceof CsvEvent.Text) {
                    buffer.append(CsvEvent.Text.class.cast(event).getText());
                }
                return none();
            }
        };
    }

    @Override
    public Class<? extends CsvEvent> getEventType() {
        return CsvEvent.Text.class;
    }

    @Override
    public Writer<CsvEvent, String> writer() {
        return new Writer<CsvEvent, String>() {
            @Override
            public Seq<CsvEvent> apply(String value) {
                if (value.isEmpty()) {
                    return Vector.empty();
                } else {
                    return Vector.of(CsvEvent.text(value));
                }
            }

            @Override
            public Seq<CsvEvent> reset() {
                return Vector.empty();
            }
        };
    }
}
