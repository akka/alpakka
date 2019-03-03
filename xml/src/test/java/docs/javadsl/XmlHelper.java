/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

public class XmlHelper {

  public static String asString(Node node) {
    StringWriter writer = new StringWriter();
    try {
      Transformer trans = TransformerFactory.newInstance().newTransformer();
      trans.setOutputProperty(OutputKeys.INDENT, "no");
      trans.setOutputProperty(OutputKeys.VERSION, "1.0");
      if (!(node instanceof Document)) {
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      }
      trans.transform(new DOMSource(node), new StreamResult(writer));
    } catch (final TransformerConfigurationException ex) {
      throw new IllegalStateException(ex);
    } catch (final TransformerException ex) {
      throw new IllegalArgumentException(ex);
    }
    return writer.toString();
  }
}
