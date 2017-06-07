package akka.stream.alpakka.marshal.xml;

import java.util.List;
import java.util.function.Function;

import javax.xml.namespace.QName;
import javax.xml.stream.events.XMLEvent;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;

import javaslang.Function1;
import javaslang.collection.Vector;
import javaslang.control.Option;

public class TagProtocol<T> implements Protocol<XMLEvent,T> {
    private final TagReadProtocol<T> read;
    private final TagWriteProtocol<T> write;

    public TagProtocol(Option<QName> name, Protocol<XMLEvent,T> protocol) {
        this(new TagReadProtocol<>(name, protocol), new TagWriteProtocol<>(name, Vector.of(protocol), Vector.of(Function1.identity())));
    }
    
    public TagProtocol(Option<QName> name, Vector<Protocol<XMLEvent,?>> protocols, Function<List<?>, T> produce, Vector<Function1<T, ?>> getters) {
        this(new TagReadProtocol<>(name, protocols, produce), new TagWriteProtocol<>(name, protocols, getters));
    }
    
    private TagProtocol(TagReadProtocol<T> read, TagWriteProtocol<T> write) {
        this.read = read;
        this.write = write;
    }

    @Override
    public Writer<XMLEvent,T> writer() {
        return write.writer();
    }

    @Override
    public Class<? extends XMLEvent> getEventType() {
        return write.getEventType();
    }
    
    @Override
    public Reader<XMLEvent,T> reader() {
        return read.reader();
    }
    
    @Override
    public String toString() {
        return read.toString();
    }
    
    public <U> TagProtocol<T> having(Protocol<XMLEvent,U> nestedProtocol, U value) {
        return new TagProtocol<>(read.having(nestedProtocol, value), write.having(nestedProtocol, value));
    }
}
 