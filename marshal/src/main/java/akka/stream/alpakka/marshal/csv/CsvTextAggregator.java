package akka.stream.alpakka.marshal.csv;

import java.util.ArrayList;
import java.util.List;

import akka.stream.alpakka.marshal.csv.CsvEvent.Text;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

/**
 * Aggregates subsequent CsvEvent.text() events for the same column value into single text event, up to a given limit
 */
public class CsvTextAggregator extends GraphStage<FlowShape<CsvEvent, CsvEvent>> {
    public static CsvTextAggregator create() {
        return new CsvTextAggregator(65535);
    }

    public static CsvTextAggregator create(int maxCharsPerEvent) {
        return new CsvTextAggregator(maxCharsPerEvent);
    }

    private final Inlet<CsvEvent> in = Inlet.create("in");
    private final Outlet<CsvEvent> out = Outlet.create("out");
    private final FlowShape<CsvEvent, CsvEvent> shape = FlowShape.of(in, out);

    private final int maxCharsPerEvent;

    private CsvTextAggregator(int maxCharsPerEvent) {
        this.maxCharsPerEvent = maxCharsPerEvent;
    }

    @Override
    public FlowShape<CsvEvent, CsvEvent> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes arg0) throws Exception {
        return new GraphStageLogic(shape) {
            List<CsvEvent.Text> buf = new ArrayList<>();

            {
                setHandlers(in, out, new AbstractInOutHandler() {
                    @Override
                    public void onPull() {
                        pull(in);
                    }

                    @Override
                    public void onPush() {
                        CsvEvent event = grab(in);
                        if (event instanceof CsvEvent.Text) {
                            Text text = CsvEvent.Text.class.cast(event);
                            if (!text.getText().isEmpty()) {
                                buf.add(text);
                            }
                            pull(in);
                        } else {
                            emitBuf();
                            emit(out, event);
                        }
                    }

                    public void onUpstreamFinish() throws Exception {
                        emitBuf();
                        completeStage();
                    }

                    private void emitBuf() {
                        if (!buf.isEmpty()) {
                            if (buf.size() == 1) {
                                emit(out, buf.get(0));
                            } else {
                                StringBuilder text = new StringBuilder();
                                for (CsvEvent.Text event: buf) {
                                    text.append(event.getText());
                                    if (text.length() > maxCharsPerEvent) {
                                        throw new IllegalArgumentException("Column value exceeds maximum of " + maxCharsPerEvent + " characters.");
                                    }
                                }
                                emit(out, CsvEvent.text(text.toString()));
                            }
                            buf.clear();
                        }
                    }
                });
            }
        };
    }

}
