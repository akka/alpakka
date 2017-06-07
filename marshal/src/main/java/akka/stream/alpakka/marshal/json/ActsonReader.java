package akka.stream.alpakka.marshal.json;

import static javaslang.control.Option.none;
import static javaslang.control.Option.some;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.alpakka.marshal.generic.Locator;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.util.ByteString;
import de.undercouch.actson.JsonEvent;
import de.undercouch.actson.JsonParser;
import javaslang.control.Option;

/**
 * Wraps the Actson JSON parser as an akka flow of ByteString to JSONEvent
 * 
 * @see https://www.michel-kraemer.com/actson-reactive-json-parser/
 */
public class ActsonReader extends GraphStage<FlowShape<ByteString,JSONEvent>> {
    public static final Locator<JSONEvent> locator = evt -> ""; // TODO location reporting for JSON events
    
    private final Inlet<ByteString> in = Inlet.create("in");
    private final Outlet<JSONEvent> out = Outlet.create("out");
    private final FlowShape<ByteString, JSONEvent> shape = FlowShape.of(in, out);

    public static final ActsonReader instance = new ActsonReader();
    
    @Override
    public FlowShape<ByteString, JSONEvent> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes attr) throws Exception {
        JsonParser parser = new JsonParser();
        return new GraphStageLogic(shape) {
            {
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        List<JSONEvent> events = new ArrayList<>();
                        parseInto(events);
                        if (events.isEmpty()) {
                            pull(in);
                        } else {
                            emitMultiple(out, events.iterator());
                        }
                    }
                });
                
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        List<JSONEvent> events = new ArrayList<>();
                        ByteString bytes = grab(in);
                        // TODO either PR ByteBuffer support into actson, or sneaky-access the underlying byte array fields here
                        for (ByteBuffer b: bytes.getByteBuffers()) {
                            byte[] buf= new byte[b.remaining()];
                            b.get(buf, 0, b.remaining());
                            int i = 0;
                            while (i < buf.length) {
                              i += parser.getFeeder().feed(buf, i, buf.length - i);
                              parseInto(events);
                            }
                        }
                        if (events.isEmpty()) {
                            pull(in);
                        } else {
                            emitMultiple(out, events.iterator());
                        }
                    }
                    
                    public void onUpstreamFinish() throws Exception {
                        parser.getFeeder().done();
                        List<JSONEvent> events = new ArrayList<>();
                        parseInto(events);
                        emitMultiple(out, events.iterator());
                        complete(out);
                    }
                });
            }
            
            private void parseInto(List<JSONEvent> events) {
                Option<JSONEvent> evt = next();
                while (evt.isDefined()) {
                    events.add(evt.get());
                    evt = next();
                }
            };
            
            private Option<JSONEvent> next() {
                switch(parser.nextEvent()) {
                case JsonEvent.END_ARRAY: return some(JSONEvent.END_ARRAY);
                case JsonEvent.END_OBJECT: return some(JSONEvent.END_OBJECT);
                case JsonEvent.ERROR: throw new IllegalArgumentException("There was a parse error at around character " + parser.getParsedCharacterCount());
                case JsonEvent.EOF: return none();
                case JsonEvent.FIELD_NAME: return some(new JSONEvent.FieldName(parser.getCurrentString()));
                case JsonEvent.NEED_MORE_INPUT: return none();
                case JsonEvent.START_ARRAY: return some(JSONEvent.START_ARRAY);
                case JsonEvent.START_OBJECT: return some(JSONEvent.START_OBJECT);
                case JsonEvent.VALUE_DOUBLE: return some(new JSONEvent.NumericValue(String.valueOf(parser.getCurrentDouble())));
                case JsonEvent.VALUE_FALSE: return some(JSONEvent.FALSE);
                case JsonEvent.VALUE_INT: return some(new JSONEvent.NumericValue(String.valueOf(parser.getCurrentInt())));
                case JsonEvent.VALUE_NULL: return some(JSONEvent.NULL);
                case JsonEvent.VALUE_STRING: return some(new JSONEvent.StringValue(parser.getCurrentString()));
                case JsonEvent.VALUE_TRUE: return some(JSONEvent.TRUE);
                default: throw new UnsupportedOperationException("Unexpected event in JSON parser");
                }
            }
        };
    }
}
