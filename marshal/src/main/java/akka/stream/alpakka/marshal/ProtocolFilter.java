package akka.stream.alpakka.marshal;

import static javaslang.control.Option.none;
import static javaslang.control.Option.some;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.NotUsed;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import javaslang.collection.Vector;
import javaslang.control.Option;
import javaslang.control.Try;

/**
 * Selectively routes events that are matched by a {@link ReadProtocol} into a separate flow, allowing
 * unmatched events to pass unchanged.
 */
public class ProtocolFilter {
    private static final Logger log = LoggerFactory.getLogger(ProtocolFilter.class);
    
    private static class Marker {}
    private static final Marker SELECTED = new Marker();
    private static final Marker NOT_SELECTED = new Marker();
    
    /**
     * Creates a flow that feeds each event into [selector], and:
     * - If the selector emits a result, that result, and further results that also match, are fed through [target], and then merged into the stream.
     * - If the selector emits {@link ReadProtocol#none()}, the event is passed downstream unchanged
     * - If the selector emits a failure, the stream is failed.
     */
    public static <E> Flow<E, E, NotUsed> filter(ReadProtocol<E,E> selector, Graph<FlowShape<E,E>, ?> target) {
        return Flow
            .<E>create()
            .via(insertMarkers(selector))
            .splitWhen(Marker.class::isInstance)
            .prefixAndTail(1)
            .flatMapConcat(t -> {
                // This is safe because all of the "tail" in "prefixAndTail" above comes from the original stream of E.
                Source<E, NotUsed> source = uncheckedCast(t.second());
                return isSelected(t.first().get(0)) ? source.via(target) : source;
            })
            .concatSubstreams();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static <E> Source<E, NotUsed> uncheckedCast(Source<Object, NotUsed> second) {
        return (Source) second;
    }

    private static boolean isSelected(Object marker) {
        return (Marker.class.cast(marker) == SELECTED);
    }

    /**
     * Inserts marker objects into the stream whenever [selector] starts or stops selecting.
     *  - inserts SELECTED before events where [selector] started selecting
     *  - inserts NOT_SELECTED before events where [selector] stopped selecting
     * The output stream will always start with a marker object.
     */
    private static <E> GraphStage<FlowShape<E,Object>> insertMarkers(ReadProtocol<E,E> selector) {
        return new GraphStage<FlowShape<E,Object>>() {
            private final Inlet<E> in = Inlet.create("in");
            private final Outlet<Object> out = Outlet.create("out");
            private final FlowShape<E, Object> shape = FlowShape.of(in, out);

            @Override
            public FlowShape<E, Object> shape() {
                return shape;
            }

            @Override
            public GraphStageLogic createLogic(Attributes attr) {
                return new GraphStageLogic(shape) {
                    private final Reader<E,E> reader = selector.reader();
                    private Option<Boolean> prevSelected = none();
                    {
                        setHandler(in, new AbstractInHandler(){
                            @Override
                            public void onPush() {
                                final E event = grab(in);
                                final Try<E> result = reader.apply(event);
                                final boolean selected = !ReadProtocol.isNone(result);
                                log.debug("in {}, reader {}", event, result);
                                
                                if (selected && result.isFailure()) {
                                    failStage(result.failed().get());
                                } else {
                                    if (prevSelected.filter(p -> p == selected).isDefined()) {
                                        // no new marker needed
                                        if (selected) {
                                            push(out, result.get());
                                        } else {
                                            push(out, event);
                                        }
                                    } else {
                                        log.debug("New marker! selected={}", selected);
                                        // insert a marker since the selector has changed outputting
                                        if (selected) {
                                            emitMultiple(out, Vector.of(SELECTED, result.get()).iterator());
                                        } else {
                                            emitMultiple(out, Vector.of(NOT_SELECTED, event).iterator());
                                        }
                                    }
                                    prevSelected = some(selected);
                                }
                            }
                        });
                        
                        setHandler(out, new AbstractOutHandler() {
                            @Override
                            public void onPull() {
                                pull(in);
                            }
                        });
                    }
                };
            }
        };
    }
    
}
