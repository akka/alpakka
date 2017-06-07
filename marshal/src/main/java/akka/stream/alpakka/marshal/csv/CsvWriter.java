package akka.stream.alpakka.marshal.csv;

import java.nio.charset.CharacterCodingException;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

/**
 * Renders an incoming stream of CSV events into string content, according to the given settings.
 */
public class CsvWriter extends GraphStage<FlowShape<CsvEvent, String>> {
    private final Inlet<CsvEvent> in = Inlet.create("in");
    private final Outlet<String> out = Outlet.create("out");
    private final FlowShape<CsvEvent, String> shape = FlowShape.of(in, out);

    private final CsvSettings settings;
    private final String quote;
    private final String separator;
    
    public CsvWriter(CsvSettings settings) {
        this.settings = settings;
        this.quote = String.valueOf(settings.getQuote());
        this.separator = String.valueOf(settings.getSeparator());
    }

    @Override
    public FlowShape<CsvEvent, String> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes arg0) {
        return new GraphStageLogic(shape) {
            boolean startedValue = false;
            boolean needSeparator = false;
            
            {
                setHandlers(in, out, new AbstractInOutHandler() {
                    @Override
                    public void onPull() {
                        pull(in);
                    }
                    
                    @Override
                    public void onPush() throws CharacterCodingException {
                        CsvEvent e = grab(in);
                        if (e instanceof CsvEvent.EndRecord) {
                            if (startedValue) {
                                emit(out, quote);
                            }
                            emit(out, "\n");
                            startedValue = false;
                            needSeparator = false;
                        } else if (e instanceof CsvEvent.EndValue) {
                            if (startedValue) {
                                emit(out, quote);
                            } else {
                                // empty field
                                if (needSeparator) {
                                    emit(out, separator);
                                } else {
                                    pull(in);
                                }
                            }
                            startedValue = false;
                            needSeparator = true;
                        } else {
                            String s = CsvEvent.Text.class.cast(e).getText();
                            if (s.length() > 0) {
                                if (!startedValue) {
                                    if (needSeparator) {
                                        emit(out, separator);
                                    }
                                    emit(out, quote);
                                }
                                emit(out, settings.escape(s));
                                startedValue = true;
                            } else {
                                pull(in);
                            }
                        }
                    }
                });
            }
        };
    }
}
