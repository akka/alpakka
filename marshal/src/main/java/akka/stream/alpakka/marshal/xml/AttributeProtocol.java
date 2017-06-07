package akka.stream.alpakka.marshal.xml;

import static akka.stream.alpakka.marshal.ReadProtocol.none;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.XMLEvent;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.generic.StringProtocol;
import akka.stream.alpakka.marshal.Writer;

import javaslang.collection.Vector;
import javaslang.control.Try;

/**
 * Handles reading and writing a single attribute of a tag.
 */
public class AttributeProtocol extends StringProtocol<XMLEvent> {
    private static final XMLEventFactory factory = XMLEventFactory.newFactory();
    
    public AttributeProtocol(QName name) {
        super(new Protocol<XMLEvent,String>() {
            Writer<XMLEvent,String> writer = Writer.of(s -> Vector.of(factory.createAttribute(name, s)));
            @Override
            public Class<? extends XMLEvent> getEventType() {
                return Attribute.class;
            }
            
            @Override
            public String toString() {
                return "@" + name;
            }

            @Override
            public Reader<XMLEvent,String> reader() {
                return new Reader<XMLEvent,String>() {
                    private int level = 0;

                    @Override
                    public Try<String> reset() {
                        level = 0;
                        return none();
                    }

                    @Override
                    public Try<String> apply(XMLEvent evt) {
                        if (level == 0 && evt.isAttribute() && matches(Attribute.class.cast(evt))) {
                            return Try.success(Attribute.class.cast(evt).getValue());
                        } else if (evt.isStartElement()) {
                            level++;
                            return none();
                        } else if (evt.isEndElement()) {
                            level--;
                            return none();
                        } else {
                            return none();
                        }
                    }

                    private boolean matches(Attribute attr) {
                        return name.equals(attr.getName());
                    }
                    
                };
            }
            
            @Override
            public Writer<XMLEvent,String> writer() {
                return writer;
            }
        }, AaltoReader.locator);
    }
}
