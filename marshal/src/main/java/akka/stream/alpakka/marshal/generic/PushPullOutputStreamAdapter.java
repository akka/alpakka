package akka.stream.alpakka.marshal.generic;

import java.io.OutputStream;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.util.ByteString;

/**
 * Adapts a writer W that can write to an OutputStream to serve in an akka stream in a non-blocking fashion.
 * The writer W is expected to write to an OutputStream as a result of receiving elements of type T. For example,
 * W could be an XML writer emitting bytes for every XML event.
 * 
 * W does not have to be multi-thread safe; it can be assumed to only be operated upon by one thread at a time.
 * 
 * @param <T> The input element type that is given to W
 * @param <W> The writer object that transforms T into calls on an output stream
 */
public class PushPullOutputStreamAdapter<T,W> extends GraphStage<FlowShape<T, ByteString>> {
    private final Outlet<ByteString> out = Outlet.create("out");
    private final Inlet<T> in = Inlet.create("in");
    private final FlowShape<T, ByteString> shape = FlowShape.of(in, out);
    
    private final CheckedBiFunction<Attributes,OutputStream,W> factory;
    private final CheckedBiConsumer<W,T> write;

    /**
     * Creates a new PushPullOutputStreamAdapter
     * @param factory Lambda that will create a new writer W based on stream attributes and a target output stream.
     * @param write Lamda that will tell a writer to to write an element T to its output stream.
     */
    public PushPullOutputStreamAdapter(CheckedBiFunction<Attributes, OutputStream, W> factory, CheckedBiConsumer<W, T> write) {
        this.factory = factory;
        this.write = write;
    }

    @Override
    public FlowShape<T, ByteString> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes attr) throws Exception {
        ByteStringOutputStream stream = new ByteStringOutputStream();
        W writer = factory.apply(attr, stream);
        return new GraphStageLogic(shape) {{
            setHandler(out, new AbstractOutHandler() {
                @Override
                public void onPull() throws Exception {
                    if (stream.hasBytes()) {
                        push(out, stream.getBytesAndReset());
                    } else {
                        pull(in);
                    }
                }
            });
            
            setHandler(in, new AbstractInHandler() {
                @Override
                public void onPush() throws Exception {
                    T t = grab(in);
                    write.accept(writer, t);
                    if (stream.hasBytes()) {
                        push(out, stream.getBytesAndReset());
                    } else {
                        pull(in);
                    }
                }
            });
        }};
    }
    
    @FunctionalInterface
    public interface CheckedBiConsumer<T, U> {
        void accept(T t, U u) throws Exception;
    }
    
    @FunctionalInterface
    public interface CheckedBiFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }
    
}
