package akka.stream.alpakka.marshal.xml;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.XMLEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.WriteProtocol;
import akka.stream.alpakka.marshal.Writer;

import javaslang.Function1;
import javaslang.Tuple2;
import javaslang.collection.Seq;
import javaslang.collection.Vector;
import javaslang.control.Option;

@SuppressWarnings("unchecked")
public class TagWriteProtocol<T> implements WriteProtocol<XMLEvent,T> {
    private static final Logger log = LoggerFactory.getLogger(TagWriteProtocol.class);
    private static final XMLEventFactory factory = XMLEventFactory.newFactory();
    
    private final Function<T,QName> getName;
    private final Seq<WriteProtocol<XMLEvent,?>> attrProtocols;
    private final Seq<WriteProtocol<XMLEvent,?>> otherProtocols;
    private final Seq<Function1<T,?>> attrGetters;
    private final Seq<Function1<T,?>> otherGetters;
    private final Option<QName> name;
    
    /**
     * @param name The qualified name of the tag to write, or none() to have the last item of [getters] deliver a {@link QName}.
     * @param getters Getter function for each sub-protocol to write (and additional first element delivering a QName, if name == none())
     * @param protocols Protocols to use to write each of the getter elements
     */
    public TagWriteProtocol(Option<QName> name, Vector<? extends WriteProtocol<XMLEvent,?>> protocols, Vector<Function1<T, ?>> g) {
        if (name.isDefined() && (protocols.size() != g.size()) ||
            name.isEmpty() && (protocols.size() != g.size() - 1)) {
            throw new IllegalArgumentException ("Number of protocols and getters does not match");
        }
        this.name = name;
        this.getName = t -> name.getOrElse(() -> (QName) g.head().apply(t));
        
        Vector<Function1<T, ?>> getters = (name.isEmpty()) ? g.drop(1) : g;
        
        Tuple2<Vector<Tuple2<WriteProtocol<XMLEvent,?>, Function1<T, ?>>>, Vector<Tuple2<WriteProtocol<XMLEvent,?>, Function1<T, ?>>>> partition =
            ((Vector<WriteProtocol<XMLEvent,?>>)protocols).zip(getters)
            .partition(t -> Attribute.class.isAssignableFrom(t._1.getEventType()));
        
        this.attrProtocols = partition._1().map(t -> t._1());
        this.attrGetters = partition._1().map(t -> t._2());
        this.otherProtocols = partition._2().map(t -> t._1());
        this.otherGetters = partition._2().map(t -> t._2());
    }

    private TagWriteProtocol(Option<QName> name, Function<T, QName> getName, Seq<WriteProtocol<XMLEvent,?>> attrProtocols,
        Seq<WriteProtocol<XMLEvent,?>> otherProtocols, Seq<Function1<T, ?>> attrGetters, Seq<Function1<T, ?>> otherGetters) {
        this.name = name;
        this.getName = getName;
        this.attrProtocols = attrProtocols;
        this.otherProtocols = otherProtocols;
        this.attrGetters = attrGetters;
        this.otherGetters = otherGetters;
    }
    
    public <U> TagWriteProtocol<T> having(WriteProtocol<XMLEvent,U> nestedProtocol, U value) {
        return Attribute.class.isAssignableFrom(nestedProtocol.getEventType())
            ? new TagWriteProtocol<>(name, getName, attrProtocols.append(nestedProtocol), otherProtocols, attrGetters.append(t -> value), otherGetters)
            : new TagWriteProtocol<>(name, getName, attrProtocols, otherProtocols.append(nestedProtocol), attrGetters, otherGetters.append(t -> value));
    }

    @Override
    public Writer<XMLEvent,T> writer() {
        return new Writer<XMLEvent, T>() {
            boolean started = false;
            private EndElement endElement;
            
            @Override
            public Seq<XMLEvent> apply(T value) {
                log.debug("{}: Writing {}", TagWriteProtocol.this, value);
                Seq<XMLEvent> prefix = (started) ? Vector.empty() : Vector.of(startElement(value));
                started = true;
                endElement = factory.createEndElement(getName.apply(value), null);
                
                return prefix.appendAll(
                    Vector.range(0, otherProtocols.size()).map(i -> {
                        Writer<XMLEvent,Object> w = (Writer<XMLEvent,Object>) otherProtocols.get(i).writer();
                        return w.applyAndReset(otherGetters.get(i).apply(value));
                    }).flatMap(Function.identity())
                );
            }

            @Override
            public Seq<XMLEvent> reset() {
                log.debug("{}: Resetting", TagWriteProtocol.this);
                if (started) {
                    started = false;
                    return Vector.of(endElement);
                } else {
                    return Vector.empty();
                }
            }
        };
    }
    
    @Override
    public Class<? extends XMLEvent> getEventType() {
        return XMLEvent.class;
    }
    
    @Override
    public String toString() {
        StringBuilder msg = new StringBuilder("<");
        msg.append(name.map(Object::toString).getOrElse("*"));
        msg.append(">");
        Seq<WriteProtocol<XMLEvent,?>> protocols = attrProtocols.appendAll(otherProtocols);
        if (!protocols.isEmpty()) {
            msg.append(" with ");
            msg.append(protocols.map(p -> p.toString()).mkString(", "));
        }
        return msg.toString();
   }
    
    private XMLEvent startElement(T value) {
        List<Attribute> attributes = new ArrayList<>();
        for (int i = 0; i < attrGetters.size(); i++) {
            Object o = attrGetters.get(i).apply(value);
            WriteProtocol<XMLEvent,Object> attributeProtocol = (WriteProtocol<XMLEvent,Object>) attrProtocols.get(i);
            attributeProtocol.writer().apply(o).map(Attribute.class::cast).forEach(attributes::add);
        }
        return factory.createStartElement(getName.apply(value), attributes.iterator(), null);
    }
}
 