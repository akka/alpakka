package akka.stream.alpakka.marshal.xml;

import javax.xml.stream.Location;

public class XMLReadException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public static XMLReadException wrap(RuntimeException cause, Location location) {
        if (cause instanceof XMLReadException) {
            return (XMLReadException) cause;
        } else {
            return new XMLReadException(cause, location);
        }
    }
    
    private XMLReadException(RuntimeException cause, Location location) {
        super(cause.getMessage() + " at " + location.getLineNumber() + ":" + location.getColumnNumber(), cause);
    }
}
