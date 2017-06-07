package akka.stream.alpakka.marshal.xml;

import static akka.stream.alpakka.marshal.ReadProtocol.none;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.events.XMLEvent;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.generic.StringProtocol;
import akka.stream.alpakka.marshal.Writer;

import javaslang.collection.Vector;
import javaslang.control.Try;

/**
 * Represents the character data body at root level as a String. If during reading, an empty tag or no body is encountered,
 * no value is emitted. In other words, no empty strings will be emitted during reading.
 */
public class BodyProtocol extends StringProtocol<XMLEvent> {
    public static final BodyProtocol INSTANCE = new BodyProtocol();
    
    private static final XMLEventFactory factory = XMLEventFactory.newFactory();
    
    private BodyProtocol() {
        super(new Protocol<XMLEvent,String>(){
            Writer<XMLEvent,String> writer = Writer.of(value -> Vector.of(factory.createCharacters(value)));
            
            @Override
            public Writer<XMLEvent,String> writer() {
                return writer;
            }
            
            @Override
            public Class<? extends XMLEvent> getEventType() {
                return XMLEvent.class;
            }

            @Override
            public Reader<XMLEvent,String> reader() {
                return new Reader<XMLEvent,String>() {
                    private int level = 0;
                    private final List<String> buffer = new ArrayList<>();
                    
                    @Override
                    public Try<String> reset() {
                        level = 0;
                        Try<String> result = Try.success(buffer.stream().collect(Collectors.joining()));
                        buffer.clear();
                        return result.get().isEmpty() ? none() : result;
                    }

                    @Override
                    public Try<String> apply(XMLEvent evt) {
                        if (level == 0 && evt.isCharacters()) {
                            buffer.add(evt.asCharacters().getData());
                            return none();
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
                    
                };
            }
            
            @Override
            public String toString() {
                return "body";
            }
        }, AaltoReader.locator);
    }
}
