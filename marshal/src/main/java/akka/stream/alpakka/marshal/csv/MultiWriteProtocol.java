package akka.stream.alpakka.marshal.csv;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.Writer;

import javaslang.Function1;
import javaslang.collection.Seq;
import javaslang.collection.Vector;

/**
 * Groups nested {@see WriteProtocol} protocols to write a combined file.
 * 
 * When writing a T, it will allow each nested protocol to contribute column(s) to a line, until it emits endRecord().
 * The endRecord() of each contributing protocol is not forwarded though; rather, the next contributing protocol is then
 * allowed to append to the line. Only after all protocols have emitted for one T, is an endRecord() emitted.
 * 
 * The inner records must also properly emit endValue() before endRecord().
 */
public class MultiWriteProtocol<T> implements WriteProtocol<CsvEvent, T> {
    private static final Logger log = LoggerFactory.getLogger(MultiWriteProtocol.class);
    
    private final Seq<WriteProtocol<CsvEvent,?>> protocols;
    private final Seq<Function1<T,?>> getters;

    public MultiWriteProtocol(Iterable<WriteProtocol<CsvEvent,?>> protocols, Iterable<Function1<T,?>> getters) {
        this.protocols = Vector.ofAll(protocols);
        this.getters = Vector.ofAll(getters);
    }

    @Override
    public Class<? extends CsvEvent> getEventType() {
        return CsvEvent.class;
    }

    @Override
    public Writer<CsvEvent, T> writer() {
        @SuppressWarnings("unchecked")
        Seq<Writer<CsvEvent, Object>> writers = protocols.map(c -> (Writer<CsvEvent,Object>) c.writer());
        
        return new Writer<CsvEvent,T>() {
            @Override
            public Seq<CsvEvent> apply(T value) {
                return emit(i -> writers.apply(i).apply(getters.apply(i).apply(value)));
            }

            @Override
            public Seq<CsvEvent> reset() {
                return emit(i -> writers.apply(i).reset());
            }
            
            private Seq<CsvEvent> emit(Function1<Integer, Seq<CsvEvent>> getEvents) {
                List<Seq<CsvEvent>> resultRows = new ArrayList<>();
                for (int writerIdx = 0; writerIdx < writers.size(); writerIdx++) {
                    Seq<CsvEvent> events = getEvents.apply(writerIdx);
                    int row = 0;
                    if (events.isEmpty()) {
                        log.warn("{} did not emit any events. Emitting empty column instead.", protocols.apply(writerIdx));
                        events = Vector.of(CsvEvent.endRecord());
                    } else {
                        if (!events.endsWith(Vector.of(CsvEvent.endValue(), CsvEvent.endRecord()))) {
                            throw new IllegalArgumentException("Expecting nested writer to end its write with endValue, endRecord but did not: " + events);
                        }
                        events = events.dropRight(1);
                        while (!events.isEmpty()) {
                            Seq<CsvEvent> rowEvents = events.takeWhile(e -> !(e instanceof CsvEvent.EndRecord));
                            log.debug("Row {}, writer {}: {}", row, writerIdx, rowEvents);
                            events = events.drop(rowEvents.size());
                            if (!events.isEmpty()) {
                                // drop the endRecord() separator between two rows of a single inner write
                                events = events.drop(1);
                            }
                            while (row >= resultRows.size()) {
                                if (row == resultRows.size()) {
                                    // empty columns until current writer
                                    resultRows.add(Vector.fill(writerIdx, () -> CsvEvent.endValue()));
                                } else {
                                    // completely empty row
                                    resultRows.add(Vector.fill(writers.size(), () -> CsvEvent.endValue()));
                                }
                            }
                            resultRows.set(row, resultRows.get(row).appendAll(rowEvents));
                            row++;
                        }
                    }
                    // fill the rows that weren't emitted from this writer (but were from others) with empty columns
                    while (row < resultRows.size()) {
                        resultRows.set(row, resultRows.get(row).append(CsvEvent.endValue()));
                        row++;
                    }
                }
                return Vector.ofAll(resultRows).map(row -> row.append(CsvEvent.endRecord())).fold(Vector.empty(), (a,b) -> a.appendAll(b));
            }
        };
    }
}
