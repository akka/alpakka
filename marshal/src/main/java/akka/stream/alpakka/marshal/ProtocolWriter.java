package akka.stream.alpakka.marshal;

import static javaslang.control.Option.none;
import static javaslang.control.Option.some;

import java.util.function.Function;
import java.util.function.Supplier;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import javaslang.collection.Seq;
import javaslang.collection.Vector;
import javaslang.control.Option;

public class ProtocolWriter {
    /**
     * Transforms a stream of T into a stream of events E, by applying a {@link WriteProtocol} to each element T.
     */
    public static <T,E> GraphStage<FlowShape<T, E>> of(WriteProtocol<E,T> protocol) {
        return ProtocolWriter.using(() -> {
            Writer<E, T> writer = protocol.writer();
            return t -> writer;
        });
    }

    /**
     * Returns a graph stage which writes each element T using (potentially) its own writer that is created using
     * the given factory.
     * 
     * @param factory Supplier returning a function that returns the writer for an element. The supplier is invoked
     * each time the graph stage is materialized; the function is then invoked on each element.
     */
    public static <T,E> GraphStage<FlowShape<T, E>> using(Supplier<Function<T,Writer<E,T>>> factory) {
        return new GraphStage<FlowShape<T, E>>() {
            private final Inlet<T> in = Inlet.create("in");
            private final Outlet<E> out = Outlet.create("out");
            private final FlowShape<T, E> shape = FlowShape.of(in, out);
            
            @Override
            public FlowShape<T, E> shape() {
                return shape;
            }

            @Override
            public GraphStageLogic createLogic(Attributes attr) throws Exception {
                Function<T, Writer<E, T>> writerFn = factory.get();
                return new GraphStageLogic(shape) {
                    private Option<Writer<E,T>> writer = none();
                {
                    setHandler(out, new AbstractOutHandler() {
                        @Override
                        public void onPull() throws Exception {
                            pull(in);
                        }
                    });
                    
                    setHandler(in, new AbstractInHandler() {
                        @Override
                        public void onPush() throws Exception {
                            T t = grab(in);
                            writer = some(writerFn.apply(t));
                            Seq<E> events = writer.get().apply(t);
                            emitMultiple(out, events.iterator());
                        }
                        
                        @Override
                        public void onUpstreamFinish() throws Exception {
                            Seq<E> events = writer.map(w -> w.reset()).getOrElse(Vector.empty());
                            emitMultiple(out, events.iterator(), () -> complete(out));
                        }
                    });
                }};
            }
        };
    }
    
}
