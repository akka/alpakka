package akka.stream.alpakka.marshal.xml;

import static akka.stream.alpakka.marshal.ReadProtocol.isNone;
import static akka.stream.alpakka.marshal.ReadProtocol.none;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.xml.namespace.QName;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.XMLEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.generic.ConstantProtocol;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.generic.ValidationException;

import javaslang.collection.Seq;
import javaslang.collection.Vector;
import javaslang.control.Option;
import javaslang.control.Try;

@SuppressWarnings({"unchecked","rawtypes"})
public class TagReadProtocol<T> implements ReadProtocol<XMLEvent,T> {
    private static final Function<List<?>, Object> IDENTITY = list -> list.get(0);
    private static final Logger log = LoggerFactory.getLogger(TagReadProtocol.class);
    
    private static <T> Function<List<?>, T> identity() {
        return (Function<List<?>, T>) IDENTITY;
    }
    
    private final Option<QName> name;
    private final Vector<? extends ReadProtocol<XMLEvent,?>> protocols;
    private final Function<List<?>, T> produce;
    private final Seq<ReadProtocol<XMLEvent,ConstantProtocol.Present>> conditions;

    /**
     * Creates a new TagReadProtocol
     * @param name Name of the tag to match, or none() to match any tag ([produce] will get another argument (at position 0) with the tag's QName in that case)
     * @param protocols Attributes and child tags to read
     * @param produce Function that must accept a list of (attributes.size + tags.size) objects and turn that into T
     */
    public TagReadProtocol(Option<QName> name, Vector<? extends ReadProtocol<XMLEvent,?>> protocols, Function<List<?>, T> produce) {
        this(name, protocols, produce, Vector.empty());
    }
    
    /**
     * Reads a tag and one child element (tag or attribute), where the result of this tag is the result of the single child.
     */
    public TagReadProtocol(Option<QName> name, ReadProtocol<XMLEvent,?> protocol) {
        this(name, Vector.of(protocol), identity(), Vector.empty());
    }
    
    private TagReadProtocol(Option<QName> name, Vector<? extends ReadProtocol<XMLEvent,?>> protocols, Function<List<?>, T> produce, Seq<ReadProtocol<XMLEvent,ConstantProtocol.Present>> conditions) {
        this.name = name;
        this.protocols = protocols;
        this.produce = produce;
        this.conditions = conditions;
    }
    
    @Override
    public String toString() {
        StringBuilder msg = new StringBuilder("<");
        msg.append(name.map(Object::toString).getOrElse("*"));
        msg.append(">");
        if (!protocols.isEmpty()) {
            msg.append(" with ");
            msg.append(protocols.map(p -> p.toString()).mkString(", "));
        }
        if (!conditions.isEmpty()) {
            if (protocols.isEmpty()) {
                msg.append(" with ");
            } else {
                msg.append(", ");
            }
            msg.append(conditions.map(p -> p.toString()).mkString(", "));
        }
        return msg.toString();
    }
    
    /**
     * Returns a new protocol that, in addition, also requires the given nested protocol to be present with the given constant value
     */
    public <U> TagReadProtocol<T> having(ReadProtocol<XMLEvent,U> nestedProtocol, U value) {
        return new TagReadProtocol<>(name, protocols, produce, conditions.append(ConstantProtocol.read(nestedProtocol, value)));
    }
    
    private boolean isIdentity() {
        return conditions.isEmpty() && produce == IDENTITY;
    }
    
    @Override
    public Reader<XMLEvent,T> reader() {
        return new Reader<XMLEvent,T>() {
            private final Seq<ReadProtocol<XMLEvent,Object>> all = protocols.map(p -> (ReadProtocol<XMLEvent,Object>)p).appendAll(conditions.map(p -> ReadProtocol.widen(p)));
            private final List<Reader<XMLEvent,Object>> readers = all.map(p -> p.reader()).toJavaList();
            private final Try<Object>[] values = new Try[readers.size()];
            
            private int level = 0;
            private boolean match = false;
            
            {
                reset();
            }
            
            @Override
            public Try<T> reset() {
                level = 0;
                match = false;
                readers.forEach(r -> r.reset());
                Arrays.fill(values, none());
                for (int i = 0; i < protocols.size(); i++) {
                    values[i] = (Try<Object>) protocols.get(i).empty();
                    log.debug("{} init to {}", protocols.get(i), values[i]);
                }
                return none();
            }
            
            @Override
            public Try<T> apply(XMLEvent evt) {
                if (level == 0) {
                    if (evt.isStartElement() && name.filter(n -> !n.equals(evt.asStartElement().getName())).isEmpty()) {
                        level++;
                        match = true;
                        //forward all attributes as attribute events to all sub-readers
                        Iterator i = evt.asStartElement().getAttributes();
                        while (i.hasNext()) {
                            // default JDK implementation doesn't set Location for attributes...
                            Attribute src = (Attribute) i.next();
                            forward(new AttributeDelegate(src, evt.getLocation()));
                        }
                        return none();
                    } else if (evt.isStartElement()) {
                        level++;
                        return none();
                    } else { // character data or other non-tag, just skip
                        return none();
                    }
                } else if (match && level == 1 && evt.isEndElement()) {
                    // Wrap up and emit result
                    
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    boolean includeName = name.isEmpty() && !isIdentity();
                    Object[] args = new Object[includeName ? values.length + 1: values.length];
                    
                    if (includeName) {
                        args[0] = evt.asEndElement().getName();
                    }
                    
                    for (int i = protocols.size(); i < values.length; i++) {
                        if (isNone(values[i])) {
                            failure.set(new ValidationException("must have " + conditions.get(i - protocols.size())));
                        }
                    }
                    
                    for (int i = 0; i < all.size(); i++) {
                        Try<Object> r = readers.get(i).reset();
                        log.debug("{} reset: {}", all.get(i), r);
                        if (!isNone(r) && values[i].eq(all.get(i).empty())) {
                            values[i] = r;
                        }
                    }
                    for (int i = 0; i < all.size(); i++) {
                        log.debug("wrapup: {} -> {}", all.get(i), values[i]);
                    }
                    
                    for (int i = 0; i < protocols.size(); i++) {
                        Try<Object> t = values[i];
                        t.failed().forEach(failure::set);
                        args[includeName ? i+1 : i] = t.getOrElse((Object)null);
                    }
                    
                    Try<T> result = (failure.get() != null) ? Try.failure(failure.get()) : Try.success(produce.apply(Arrays.asList(args)));
                    reset();
                    log.debug("{} emitting {}", TagReadProtocol.this, result);
                    return result;
                } else {
                    Try<T> result = none();
                    if (match) {
                        if (isIdentity()) {
                            result = (Try<T>) readers.get(0).apply(evt);
                        } else {
                            forward(evt);
                        }
                    }
                    
                    if (evt.isStartElement()) {
                        level++;
                    } else if (evt.isEndElement()) {
                        level--;
                    }
                    
                    return result;
                }
            }

            private void forward(XMLEvent evt) {
                for (int i = 0; i < readers.size(); i++) {
                    Reader<XMLEvent,Object> r = readers.get(i);
                    log.debug("Sending {} to {}", evt, all.get(i));
                    Try<Object> result = r.apply(evt);
                    log.debug("{} apply: {}", all.get(i), result);
                    if (!ReadProtocol.isNone(result)) {
                        values[i] = result;
                        log.debug("   -> {}", values[i]);
                    }
                }
            }
        };
    }
}
