package akka.stream.alpakka.marshal.json;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.generic.ConstantProtocol;
import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.Writer;

import javaslang.Function1;
import javaslang.collection.Seq;
import javaslang.collection.Vector;

/**
 * Generic class to combine several nested FieldProtocols into reading/writing a Java object instance.
 */
@SuppressWarnings("unchecked")
public class ObjectWriteProtocol<T> implements WriteProtocol<JSONEvent, T> {
    private static final Logger log = LoggerFactory.getLogger(ObjectWriteProtocol.class);
    
    private final Seq<WriteProtocol<JSONEvent, ?>> protocols;
    private final Seq<WriteProtocol<JSONEvent, ConstantProtocol.Present>> conditions;
    private final List<Function1<T, ?>> getters;

    public ObjectWriteProtocol(
        List<WriteProtocol<JSONEvent, ?>> protocols,
        List<Function1<T, ?>> getters
    ) {
        this(Vector.ofAll(protocols), getters, Vector.empty());
    }
    
    public ObjectWriteProtocol(WriteProtocol<JSONEvent, T> inner) {
        this(Vector.of((Protocol<JSONEvent, ?>)inner), Arrays.asList(Function1.identity()), Vector.empty());
    }
    
    ObjectWriteProtocol(
        Seq<WriteProtocol<JSONEvent, ?>> protocols,
        List<Function1<T, ?>> getters,
        Seq<WriteProtocol<JSONEvent, ConstantProtocol.Present>> conditions
    ) {
        this.protocols = protocols;
        this.getters = getters;
        this.conditions = conditions;
        if (protocols.size() != getters.size()) {
            throw new IllegalArgumentException("protocols must match getters");
        }
    }
    
    @Override
    public Class<? extends JSONEvent> getEventType() {
        return JSONEvent.class;
    }

    @Override
    public Writer<JSONEvent, T> writer() {
        Vector<Writer<JSONEvent, Object>> writers = Vector.range(0, protocols.size()).map(i ->
            (Writer<JSONEvent, Object>) protocols.get(i).writer());
        
        return new Writer<JSONEvent, T>() {
            boolean started = false;

            @Override
            public Seq<JSONEvent> apply(T value) {
                log.debug("{}: Writing {}", ObjectWriteProtocol.this, value);
                
                Seq<JSONEvent> events = startObject();
                for (int i = 0; i < protocols.size(); i++) {
                    events = events.appendAll(writers.get(i).apply(getters.get(i).apply(value)));
                }
                
                started = true;
                return events;
            }
            
            @Override
            public Seq<JSONEvent> reset() {
                log.debug("{}: Resetting ", ObjectWriteProtocol.this);
                
                Seq<JSONEvent> events = startObject();
                for (int i = 0; i < protocols.size(); i++) {
                    events = events.appendAll(writers.get(i).reset());
                }
                
                events = events.appendAll(conditions.map(c -> c.writer().applyAndReset(ConstantProtocol.PRESENT)).flatMap(Function.identity()));
                
                started = false;
                return events.append(JSONEvent.END_OBJECT);
            }
            
            private Seq<JSONEvent> startObject() {
                return (started) ? Vector.empty() : Vector.of(JSONEvent.START_OBJECT);
            }
        };
    }
    
    /**
     * Returns a new protocol that, in addition, writes out the given value when serializing.
     */
    public <U> ObjectWriteProtocol<T> having(WriteProtocol<JSONEvent, U> nestedProtocol, U value) {
        return new ObjectWriteProtocol<>(protocols, getters, conditions.append(ConstantProtocol.write(nestedProtocol, value)));
    }
    
    @Override
    public String toString() {
        String c = conditions.isEmpty() ? "" : ", " + conditions.mkString(", ");
        return "{ " + protocols.mkString(", ") + c + " }";
    }
}