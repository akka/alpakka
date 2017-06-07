package akka.stream.alpakka.marshal.csv;

import akka.stream.alpakka.marshal.generic.Locator;
import akka.stream.alpakka.marshal.generic.StringProtocol;

/**
 * Decorates {@link ValueProtocol} with additional String conversion functions.
 */
public class StringValueProtocol extends StringProtocol<CsvEvent> {
    private static final Locator<CsvEvent> locator = event -> "???";
    
    public static final StringValueProtocol instance = new StringValueProtocol();

    private StringValueProtocol() {
        super(ValueProtocol.instance, locator);
    }
}
