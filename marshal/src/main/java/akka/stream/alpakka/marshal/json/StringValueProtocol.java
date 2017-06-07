package akka.stream.alpakka.marshal.json;

import akka.stream.alpakka.marshal.generic.StringProtocol;

public class StringValueProtocol {
    /**
     * A JSON protocol that reads and writes strings.
     */
    public static StringProtocol<JSONEvent> INSTANCE = new StringProtocol<>(ValueProtocol.STRING, ActsonReader.locator);
}
