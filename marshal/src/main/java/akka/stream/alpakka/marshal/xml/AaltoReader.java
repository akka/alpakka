package akka.stream.alpakka.marshal.xml;

import static com.fasterxml.aalto.AsyncXMLStreamReader.EVENT_INCOMPLETE;
import static javaslang.control.Option.none;
import static javaslang.control.Option.some;
import static javax.xml.stream.XMLStreamConstants.CDATA;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.COMMENT;
import static javax.xml.stream.XMLStreamConstants.END_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;
import static javax.xml.stream.XMLStreamConstants.PROCESSING_INSTRUCTION;
import static javax.xml.stream.XMLStreamConstants.START_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;


import akka.stream.alpakka.marshal.generic.Locator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.XMLEvent;

import org.codehaus.stax2.LocationInfo;

import com.fasterxml.aalto.AsyncByteBufferFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.util.ByteString;
import javaslang.control.Option;

/**
 * Wraps the Aalto XML parser as an akka flow of ByteString to XMLEvent.
 * 
 * @see https://github.com/FasterXML/aalto-xml
 */
public class AaltoReader extends GraphStage<FlowShape<ByteString,XMLEvent>> {
    static final Locator<XMLEvent> locator = evt -> evt.getLocation().getLineNumber() + ":" + evt.getLocation().getColumnNumber();
    
    private static final AsyncXMLInputFactory factory = new InputFactoryImpl();
    
    public static final AaltoReader instance = new AaltoReader();

    private final Inlet<ByteString> in = Inlet.create("in");
    private final Outlet<XMLEvent> out = Outlet.create("out");
    private final FlowShape<ByteString, XMLEvent> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<ByteString, XMLEvent> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes attr) throws Exception {
        XMLEventFactory efactory = XMLEventFactory.newFactory();
        AsyncXMLStreamReader<AsyncByteBufferFeeder> parser = factory.createAsyncForByteBuffer();
        return new GraphStageLogic(shape) {
            {
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() {
                        try {
                            emitNext();
                        } catch (XMLStreamException x) {
                            failStage(x);
                        }
                    }
                });
                
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() {
                        try {
                            ByteString bytes = grab(in);
                            for (ByteBuffer b: bytes.getByteBuffers()) {
                                parser.getInputFeeder().feedInput(b);
                            }
                            emitNext();
                        } catch (XMLStreamException x) {
                            failStage(x);
                        }
                    }
                    
                    public void onUpstreamFinish() {
                        parser.getInputFeeder().endOfInput();
                        List<XMLEvent> events = new ArrayList<>();
                        try {
                            while (parser.hasNext()) {
                                Option<XMLEvent> n = next();
                                if (n.isDefined()) {
                                    events.add(n.get());
                                } else {
                                    emitMultiple(out, events.iterator());
                                    failStage(new XMLStreamException("Unexpected end of XML stream"));
                                    return;
                                }
                            }
                            emitMultiple(out, events.iterator());
                            completeStage();
                        } catch (XMLStreamException x) {
                            failStage(x);
                        }
                    };
                });
            }
            
            private void emitNext() throws XMLStreamException {
                if (parser.hasNext()) {
                    Option<XMLEvent> next = next();
                    // calling into Stax2 directly gives more accurate location information within a byte chunk
                    efactory.setLocation(((LocationInfo) parser).getCurrentLocation());
                    if (next.isDefined()) {
                        push(out, next.get());
                    } else {
                        pull(in);
                    }
                } else {
                    completeStage();
                }
            }
            
            private Option<XMLEvent> next() throws XMLStreamException {
                switch(parser.next()) {
                case EVENT_INCOMPLETE: return none();
                case START_DOCUMENT: return some(efactory.createStartDocument());
                case END_DOCUMENT: return some(efactory.createEndDocument());
                case START_ELEMENT: {
                    List<Attribute> attributes = new ArrayList<>();
                    for (int i = 0; i < parser.getAttributeCount(); i++) {
                        attributes.add(efactory.createAttribute(parser.getAttributeName(i), parser.getAttributeValue(i)));
                    }
                    List<Namespace> namespaces = new ArrayList<>();
                    for (int i = 0; i < parser.getNamespaceCount(); i++) {
                        namespaces.add(efactory.createNamespace(parser.getNamespacePrefix(i), parser.getNamespaceURI(i)));
                    }
                    return some(efactory.createStartElement(parser.getName(), attributes.iterator(), namespaces.iterator()));
                }
                case END_ELEMENT: {
                    List<Namespace> namespaces = new ArrayList<>();
                    for (int i = 0; i < parser.getNamespaceCount(); i++) {
                        namespaces.add(efactory.createNamespace(parser.getNamespacePrefix(i), parser.getNamespaceURI(i)));
                    }
                    return some(efactory.createEndElement(parser.getName(), namespaces.iterator()));
                }
                case CHARACTERS: return some(efactory.createCharacters(parser.getText()));
                case PROCESSING_INSTRUCTION: return some(efactory.createProcessingInstruction(parser.getPITarget(), parser.getPIData()));
                case COMMENT: return some(efactory.createComment(parser.getText()));
                case CDATA: return some(efactory.createCData(parser.getText()));
                default: throw new IllegalStateException("Unhandled event");
                }
            }
        };
    }
}
