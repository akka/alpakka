package akka.stream.alpakka.marshal;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import javaslang.control.Try;

/**
 * Transforms a stream of events E into a stream T, by applying a {@link ReadProtocol} to each event E.
 */
public class ProtocolReader<E,T> extends GraphStage<FlowShape<E, T>> {
    public static <E,T> ProtocolReader<E,T> of(ReadProtocol<E,T> protocol) {
        return new ProtocolReader<>(protocol);
    }
    
    private final Inlet<E> in = Inlet.create("in");
    private final Outlet<T> out = Outlet.create("out");
    private final FlowShape<E, T> shape = FlowShape.of(in, out);
    
    private final ReadProtocol<E,T> protocol;
    
    private ProtocolReader(ReadProtocol<E, T> protocol) {
        this.protocol = protocol;
    }

    @Override
    public FlowShape<E, T> shape() {
        return shape;
    }
    
    @Override
    public GraphStageLogic createLogic(Attributes attr) throws Exception {
        Reader<E, T> reader = protocol.reader();
        return new GraphStageLogic(shape) {{
            setHandler(out, new AbstractOutHandler() {
                @Override
                public void onPull() throws Exception {
                    pull(in);
                }
            });
            
            setHandler(in, new AbstractInHandler() {
                @Override
                public void onPush() throws Exception {
                    E event = grab(in);
                    Try<T> result = reader.apply(event);
                    if (result.isSuccess()) {
                        push(out, result.get());
                    } else if (result.isFailure() && !ReadProtocol.isNone(result)) {
                        failStage(result.failed().get());
                    } else {
                        pull(in);
                    }
                }
                
                @Override
                public void onUpstreamFinish() throws Exception {
                    Try<T> result = reader.reset();
                    if (result.isSuccess()) {
                        push(out, result.get());
                        complete(out);
                    } else if (result.isFailure() && !ReadProtocol.isNone(result)) {
                        failStage(result.failed().get());
                    } else {
                        complete(out);
                    }
                }
            });
        }};
    }
}
