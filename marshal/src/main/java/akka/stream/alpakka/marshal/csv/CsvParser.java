package akka.stream.alpakka.marshal.csv;

import java.nio.charset.CharacterCodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

/**
 * Parses an incoming stream of strings making up a CSV document into individual CSV events.
 */
public class CsvParser extends GraphStage<FlowShape<String, CsvEvent>> {
    private static final Logger log = LoggerFactory.getLogger(CsvParser.class);
    private static enum State { FIELD_START, QUOTED_FIELD, UNQUOTED_FIELD, AFTER_QUOTED_FIELD } ;
    private static final char BOM = '\uFEFF';
    
    private final Inlet<String> in = Inlet.create("in");
    private final Outlet<CsvEvent> out = Outlet.create("out");
    private final FlowShape<String, CsvEvent> shape = FlowShape.of(in, out);
    
    private final CsvSettings settings;
    
    public CsvParser(CsvSettings settings) {
        this.settings = settings;
    }

    @Override
    public FlowShape<String, CsvEvent> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes arg0) {
        return new GraphStageLogic(shape) {
            State state = State.FIELD_START;
            String buf = "";
            
            {
                setHandlers(in, out, new AbstractInOutHandler() {
                    @Override
                    public void onPull() {
                        pull(in);
                    }
                    
                    @Override
                    public void onPush() throws CharacterCodingException {
                        String s = buf + grab(in);
                        log.debug("<< {}", s);
                        buf = parseAndEmit(s);
                        if (isAvailable(out)) {
                            // apparently, we haven't emitted anything while parsing
                            pull(in);
                        }
                    }
                    
                    public void onUpstreamFinish() {
                        if (!buf.isEmpty()) {
                            // emit the remaining buffer, unless we were in a quoted field and it's just the remaining quote
                            if (state != State.QUOTED_FIELD || !buf.equals(String.valueOf(settings.getQuote()))) {
                                emit(out, CsvEvent.text(settings.unescape(buf)));
                            }
                        }
                        emit(out, CsvEvent.endValue());
                        emit(out, CsvEvent.endRecord());
                        completeStage();
                    };
                });
            }
            
            private String parseAndEmit(String s) {
                int p = (s.charAt(0) == BOM ? 1 : 0);
                while (p < s.length()) {
                    log.debug("[{}] {}", p, state);
                    switch(state) {
                        case FIELD_START:
                            if (s.charAt(p) == settings.getQuote()) {
                                p++;
                                state = State.QUOTED_FIELD;
                                if (p > s.length()) {
                                    return "";
                                }
                            } else if (s.charAt(p) == settings.getSeparator()) {
                                // empty field
                                emit(out, CsvEvent.endValue());
                                p++;
                            } else if (s.charAt(p) == '\n') {
                                // end of record
                                emit(out, CsvEvent.endValue());
                                emit(out, CsvEvent.endRecord());
                                p++;
                            } else {
                                state = State.UNQUOTED_FIELD;
                            }
                            break;
                        case UNQUOTED_FIELD:
                            int sepPos = s.indexOf(settings.getSeparator(), p);
                            int lfPos = s.indexOf('\n', p);
                            if (sepPos == -1 && lfPos == -1) {
                                if (p != s.length()) {
                                    emit(out, CsvEvent.text(s.substring(p)));
                                }
                                return "";
                            } else {
                                int q = (sepPos == -1) ? lfPos : (lfPos == -1) ? sepPos : Math.min(sepPos, lfPos);
                                if (p != q) {
                                    emit(out, CsvEvent.text(s.substring(p, q)));
                                }
                                p = q;
                                state = State.FIELD_START;
                            }
                            break;
                        case QUOTED_FIELD:
                                int q = p;
                                while (q < s.length()) {
                                    log.debug("p = {}, q = {}", p, q);
                                    if (s.length() - q < settings.getEscapedQuote().length() &&
                                        s.regionMatches(q, settings.getEscapedQuote(), 0, s.length() - q)) {
                                        if (p != q) {
                                            emit(out, CsvEvent.text(settings.unescape(s.substring(p, q))));
                                        }
                                        // partial match of escaped quote at [q], towards end of buffer
                                        return s.substring(q);
                                    } else if (s.regionMatches(q, settings.getEscapedQuote(), 0, settings.getEscapedQuote().length())) {
                                        // complete escaped quote at [q]
                                        q += settings.getEscapedQuote().length();
                                    } else if (s.charAt(q) == settings.getQuote()) {
                                        if (p != q) {
                                            emit(out, CsvEvent.text(settings.unescape(s.substring(p, q))));
                                        }
                                        p = q + 1;
                                        state = State.AFTER_QUOTED_FIELD;
                                        break;
                                    } else {
                                        q++;
                                    }
                                }
                                if (q >= s.length()) {
                                    // reached end of buffer with no end quote -> emit what we have, and pull in next buffer
                                    if (p < s.length()) {
                                        emit(out, CsvEvent.text(settings.unescape(s.substring(p))));
                                    }
                                    return "";
                                }
                            break;
                        case AFTER_QUOTED_FIELD:
                            // expect either CR/LF or a separator
                            if (s.charAt(p) != settings.getSeparator() && s.charAt(p) != '\n') {
                                throw new IllegalArgumentException("expecting SEPARATOR or NEWLINE after closing quote of field, but got '" + s.charAt(p) + "'");
                            }
                            state = State.FIELD_START;
                            break;
                    }
                }
                return "";
            }
        };
    }
}
