package akka.stream.alpakka.marshal.xml;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.UnmarshallerHandler;
import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.ReadProtocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;
import akka.stream.alpakka.marshal.xml.impl.StaxEventHandler;

import javaslang.collection.Seq;
import javaslang.collection.Vector;
import javaslang.control.Option;
import javaslang.control.Try;

/**
 * Reads and writes instances of T by delegating onto JAXB, so existing JAXB classes and/or annotations
 * can be mixed in with streams.
 */
public class JAXBProtocol<T> implements Protocol<XMLEvent,T> {
    private static final Logger log = LoggerFactory.getLogger(JAXBProtocol.class);
    
    public static <T> JAXBProtocol<T> jaxbType(Class<T> targetType) {
        try {
            return new JAXBProtocol<>(JAXBContext.newInstance(targetType), targetType.getSimpleName());
        } catch (JAXBException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static JAXBProtocol<Object> jaxbType(JAXBContext context) {
        return new JAXBProtocol<>(context, "jaxb");
    }
    
    public static <T> JAXBProtocol<T> jaxbType(Class<T> targetType, JAXBContext context) {
        return new JAXBProtocol<>(context, targetType.getSimpleName());
    }
    
    private final JAXBContext context;
    private final String name;

    private JAXBProtocol(JAXBContext context, String name) {
        this.context = context;
        this.name = name;
    }

    @Override
    public Reader<XMLEvent, T> reader() {
        try {
            UnmarshallerHandler handler = context.createUnmarshaller().getUnmarshallerHandler();
            return new Reader<XMLEvent, T>() {
                private Option<Boolean> fragment = Option.none();
                private int level = 0;

                @SuppressWarnings("unchecked")
                @Override
                public Try<T> reset() {
                    Try<T> result;
                    if (fragment.eq(Option.some(false))) {
                        result = (Try<T>) Try.of(() -> handler.getResult());
                    } else {
                        result = ReadProtocol.none();
                    }
                    fragment = Option.none();
                    level = 0;
                    log.debug("reset: {}", result);
                    return result;
                }

                @SuppressWarnings("unchecked")
                @Override
                public Try<T> apply(XMLEvent event) {
                    log.debug("Seen {}", event);
                    try {
                        // Inject a startDocument if we're reading a fragment, i.e. not seeing startDocument of the whole stream
                        if (fragment.isEmpty()) {
                            fragment = Option.some(!event.isStartDocument());
                            log.debug("Starting unmarshalling. Fragment={}", fragment.get());
                            if (fragment.get()) {
                                handler.startDocument();
                            }
                        }
                        if (event.isStartElement()) {
                            level++;
                        } else if (event.isEndElement()) {
                            level--;
                        }
                        Stax.apply(event, handler);
                        if (fragment.get() && event.isEndElement() && level == 0) {
                            try {
                                handler.endDocument();
                            } catch (SAXException e) {
                                throw new IllegalStateException(e);
                            }
                            return (Try<T>) Try.of(() -> handler.getResult());
                        }
                    } catch (SAXException e) {
                        throw new IllegalStateException(e);
                    }
                    return ReadProtocol.none();
                }
            };
        } catch (JAXBException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Class<? extends XMLEvent> getEventType() {
        return XMLEvent.class;
    }

    @Override
    public Writer<XMLEvent, T> writer() {
        try {
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
            return new Writer<XMLEvent, T>() {
                @Override
                public Seq<XMLEvent> apply(T value) {
                    List<XMLEvent> events = new ArrayList<>();
                    try {
                        marshaller.marshal(value, new StaxEventHandler(new XMLEventWriter() {
                            @Override
                            public void setPrefix(String prefix, String uri) throws XMLStreamException {
                            }
                            
                            @Override
                            public void setNamespaceContext(NamespaceContext context) throws XMLStreamException {
                            }
                            
                            @Override
                            public void setDefaultNamespace(String uri) throws XMLStreamException {
                            }
                            
                            @Override
                            public String getPrefix(String uri) throws XMLStreamException {
                                throw new UnsupportedOperationException();
                            }
                            
                            @Override
                            public NamespaceContext getNamespaceContext() {
                                throw new UnsupportedOperationException();
                            }
                            
                            @Override
                            public void flush() throws XMLStreamException {
                            }
                            
                            @Override
                            public void close() throws XMLStreamException {
                            }
                            
                            @Override
                            public void add(XMLEventReader reader) throws XMLStreamException {
                                while (reader.hasNext()) {
                                    add(reader.nextEvent());
                                }
                            }
                            
                            @Override
                            public void add(XMLEvent event) throws XMLStreamException {
                                events.add(event);
                            }
                        }));
                    } catch (JAXBException e) {
                        throw new RuntimeException(e);
                    }
                    return Vector.ofAll(events);
                }

                @Override
                public Seq<XMLEvent> reset() {
                    return Vector.empty();
                }
            };
        } catch (JAXBException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
