package akka.stream.alpakka.marshal.xml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.XMLEvent;

import akka.NotUsed;
import akka.stream.alpakka.marshal.generic.PushPullOutputStreamAdapter;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;

/**
 * Serializes XMLEvents out as XML by invoking Stax, but in a non-blocking push/pull fashion,
 * only writing when there is demand from downstream.
 */
public class StaxWriter extends PushPullOutputStreamAdapter<List<XMLEvent>, XMLEventWriter> {
    private static final XMLOutputFactory factory = XMLOutputFactory.newInstance();
    private static final XMLEventFactory evtFactory = XMLEventFactory.newFactory();
    
    /**
     * Returns a flow that buffers up to 100 XML events and writing them together.
     */
    public static Flow<XMLEvent,ByteString,NotUsed> flow() {
        return flow(100);
    }
    
    /**
     * Returns a flow that buffers up to [maximumBatchSize] XML events and writing them together.
     * 
     * Buffering is only done if upstream is faster than downstream. If there is demand from downstream,
     * also slower batches will be written.
     */
    public static Flow<XMLEvent,ByteString,NotUsed> flow(int maximumBatchSize) {
        return Flow.of(XMLEvent.class)
            .batch(maximumBatchSize, event -> {
                List<XMLEvent> l = new ArrayList<>();
                l.add(event);
                return l;
            }, (list, event) -> {
                list.add(event);
                return list;
            }).via(new StaxWriter());
    }
    
    private StaxWriter() {
        super(
            (attr, out) -> factory.createXMLEventWriter(out, "UTF-8"),
            (writer, events) -> {
                for (XMLEvent event: events) {
                    if (event.isStartElement() && !event.asStartElement().getNamespaces().hasNext()) {
                        List<Namespace> namespaces = new ArrayList<>();
                        register(writer, namespaces, event.asStartElement().getName());
                        
                        Iterator<?> attributes = event.asStartElement().getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attr = (Attribute) attributes.next();
                            register(writer, namespaces, attr.getName());
                        }
                        
                        if (!namespaces.isEmpty()) {
                            event = evtFactory.createStartElement(event.asStartElement().getName(), event.asStartElement().getAttributes(), namespaces.iterator());
                        }
                    }
                    writer.add(event);
                }
                writer.flush();
            }
        );
    }
    
    /**
     * Adds a namespace mapping for [name] to [namespaces], if it doesn't already exist on [writer].
     */
    private static void register(XMLEventWriter writer, List<Namespace> namespaces, QName name) {
        String nsUri = name.getNamespaceURI();
        if (nsUri != null && !nsUri.isEmpty()) {
            String existing = writer.getNamespaceContext().getNamespaceURI(name.getPrefix());
            if (existing == null || !existing.equals(nsUri)) {
                namespaces.add(evtFactory.createNamespace(name.getPrefix(), nsUri));
            }
        }
    }
}
