package akka.stream.alpakka.marshal.csv;

import static akka.stream.alpakka.marshal.ReadProtocol.none;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;

import javaslang.control.Try;

/**
 * Reads and writes a named column (the name being set by the first line in the file), delegating to
 * an inner protocol for all the text that is in that column.
 * 
 * If several columns are declared to have the same name, the first one is used.
 */
public class NamedColumnReadProtocol<T> implements ReadProtocol<CsvEvent,T> {
    private static final Logger log = LoggerFactory.getLogger(NamedColumnReadProtocol.class);
    
    private final String name;
    private final ReadProtocol<CsvEvent, T> inner;

    public NamedColumnReadProtocol(String name, ReadProtocol<CsvEvent,T> inner) {
        this.name = name;
        this.inner = inner;
    }

    @Override
    public Reader<CsvEvent, T> reader() {
        Reader<CsvEvent, T> innerReader = inner.reader();
        return new Reader<CsvEvent, T>() {
            boolean firstLine = true;
            int columnIdx = 0;
            int targetColumnIdx = -1;
            String firstLineCurrentColumn = "";
            
            @Override
            public Try<T> reset() {
                log.debug("{} resetting", NamedColumnReadProtocol.this);
                columnIdx = 0;
                return innerReader.reset();
            }

            @Override
            public Try<T> apply(CsvEvent event) {
                if (firstLine) {
                    if (event instanceof CsvEvent.Text) {
                        if (firstLineCurrentColumn.length() <= name.length()) {
                            // don't keep buffering bytes if this can never be the name we're looking for
                            firstLineCurrentColumn += CsvEvent.Text.class.cast(event).getText();
                        }
                    } else if (event instanceof CsvEvent.EndValue) {
                        if (targetColumnIdx == -1 && firstLineCurrentColumn.equals(name)) {
                            log.debug("{} found our column at {}", NamedColumnReadProtocol.this, columnIdx);
                            targetColumnIdx = columnIdx;
                        }
                        columnIdx++;
                        firstLineCurrentColumn = "";
                        return innerReader.reset();
                    } else if (event instanceof CsvEvent.EndRecord) {
                        firstLine = false;
                        columnIdx = 0;
                    }
                } else {
                    if (event instanceof CsvEvent.Text) {
                        if (columnIdx == targetColumnIdx) {
                            log.debug("{} forwarding {}", NamedColumnReadProtocol.this, event);
                            return innerReader.apply(event);
                        }
                    } else if (event instanceof CsvEvent.EndValue) {
                        columnIdx++;
                    } else if (event instanceof CsvEvent.EndRecord) {
                        columnIdx = 0;
                    }
                }
                return none();
            }
        };
    }
    
    @Override
    public String toString() {
        return name;
    }
}
