package akka.stream.alpakka.marshal.xml;

import javax.xml.namespace.QName;
import javax.xml.stream.events.XMLEvent;

import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.generic.TStringProtocol;

import javaslang.Tuple2;

public class QNameStringProtocol extends TStringProtocol<XMLEvent,QName> {

    public QNameStringProtocol(Protocol<XMLEvent,Tuple2<QName, String>> delegate) {
        super(delegate, AaltoReader.locator);
    }
    
    /**
     * Returns the protocol only reading and writing local names for _1, without namespace.
     */
    public TStringProtocol<XMLEvent,String> asLocalName() {
        return new TStringProtocol<>(this.map(t -> t.map1(QName::getLocalPart), t -> t.map1(QName::new)), AaltoReader.locator);
    }
}
