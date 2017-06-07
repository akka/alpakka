package akka.stream.alpakka.marshal.xml;

import java.io.Writer;

import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;

/**
 * Groups an Attribute with a Location since the StaX XMLEventFactory (and the default JDK implementation) seem
 * to think that attributes never have a location. With this class, we can at least point at the location of
 * the tag owning the attribute.
 */
public class AttributeDelegate implements Attribute {
    private final Attribute delegate;
    private final Location location;
    
    public AttributeDelegate(Attribute delegate, Location location) {
        this.delegate = delegate;
        this.location = location;
    }
    
    public QName getName() {
        return delegate.getName();
    }
    public String getValue() {
        return delegate.getValue();
    }
    public String getDTDType() {
        return delegate.getDTDType();
    }
    public int getEventType() {
        return delegate.getEventType();
    }
    public boolean isSpecified() {
        return delegate.isSpecified();
    }
    public Location getLocation() {
        return location;
    }
    public boolean isStartElement() {
        return delegate.isStartElement();
    }
    public boolean isAttribute() {
        return delegate.isAttribute();
    }
    public boolean isNamespace() {
        return delegate.isNamespace();
    }
    public boolean isEndElement() {
        return delegate.isEndElement();
    }
    public boolean isEntityReference() {
        return delegate.isEntityReference();
    }
    public boolean isProcessingInstruction() {
        return delegate.isProcessingInstruction();
    }
    public boolean isCharacters() {
        return delegate.isCharacters();
    }
    public boolean isStartDocument() {
        return delegate.isStartDocument();
    }
    public boolean isEndDocument() {
        return delegate.isEndDocument();
    }
    public StartElement asStartElement() {
        return delegate.asStartElement();
    }
    public EndElement asEndElement() {
        return delegate.asEndElement();
    }
    public Characters asCharacters() {
        return delegate.asCharacters();
    }
    public QName getSchemaType() {
        return delegate.getSchemaType();
    }
    public void writeAsEncodedUnicode(Writer writer) throws XMLStreamException {
        delegate.writeAsEncodedUnicode(writer);
    }
    public String toString() {
        return delegate.toString();
    }
}
