package akka.stream.alpakka.marshal.xml;

import javax.xml.stream.events.XMLEvent;
import javax.xml.validation.Schema;
import javax.xml.validation.ValidatorHandler;

import org.xml.sax.Locator;
import org.xml.sax.SAXParseException;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

/**
 * A graph stage that runs incoming XML events past an XSD schema, failing the flow
 * if an error is found.
 * 
 * TODO (1): The implementation does currently NOT augment the XML events according to the schema.
 *           For example, default values present in the schema are not applied to the events.
 *           All events are passed through unchanged.
 * 
 * TODO (2): The implementation fails the stream on the first error. It might be desirable to accumulate
 *           several/all/until timeout/... errors instead.
 */
public class SchemaValidatorFlow extends GraphStage<FlowShape<XMLEvent, XMLEvent>> {
    public static GraphStage<FlowShape<XMLEvent, XMLEvent>> of(Schema schema) {
        return new SchemaValidatorFlow(schema);
    }
    
    private final Outlet<XMLEvent> out = Outlet.create("out");
    private final Inlet<XMLEvent> in = Inlet.create("in");
    private final FlowShape<XMLEvent, XMLEvent> shape = FlowShape.of(in, out);
    private final Schema schema;

    private SchemaValidatorFlow(Schema schema) {
        this.schema = schema;
    }

    @Override
    public FlowShape<XMLEvent, XMLEvent> shape() {
        return shape;
    }
    
    @Override
    public GraphStageLogic createLogic(Attributes attr) throws Exception {
        ValidatorHandler handler = schema.newValidatorHandler();
        return new GraphStageLogic(shape) {
            int lineNumber = -1;
            int columnNumber = -1;
        {
            handler.setDocumentLocator(new Locator() {
                @Override
                public String getPublicId() {
                    return null;
                }

                @Override
                public String getSystemId() {
                    return null;
                }

                @Override
                public int getLineNumber() {
                    return lineNumber;
                }

                @Override
                public int getColumnNumber() {
                    return columnNumber;
                }
            });
            
            setHandler(out, new AbstractOutHandler() {
                @Override
                public void onPull() {
                    pull(in);
                }
            });
            
            setHandler(in, new AbstractInHandler() {
                @Override
                public void onPush() throws Exception {
                    XMLEvent event = grab(in);
                    if (event.getLocation() != null) {
                        lineNumber = event.getLocation().getLineNumber();
                        columnNumber = event.getLocation().getColumnNumber();
                    }
                    try {
                        Stax.apply(event, handler);
                    } catch (SAXParseException x) {
                        throw x;
                    }
                    push(out, event);
                }
            });
        }};
    }
}
