package akka.stream.alpakka.csv.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import scala.collection.JavaConversions;

import java.util.Collection;

public class CsvFraming {

    public static final byte backslash = '\\';
    public static final byte comma = ',';
    public static final byte doubleQuote = '"';

    public static Flow<ByteString, Collection<ByteString>, NotUsed> lineScanner() {
        return lineScanner(comma, doubleQuote, backslash);
    }

    public static Flow<ByteString, Collection<ByteString>, NotUsed> lineScanner(byte delimiter, byte quoteChar, byte escapeChar) {
        return akka.stream.alpakka.csv.scaladsl.CsvFraming
                .lineScanner(delimiter, quoteChar, escapeChar).asJava()
                .map(JavaConversions::asJavaCollection);
    }


}
