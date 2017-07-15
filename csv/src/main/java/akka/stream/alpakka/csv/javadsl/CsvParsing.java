/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import scala.collection.JavaConversions;

import java.util.Collection;

public class CsvParsing {

    public static final byte BACKSLASH = '\\';
    public static final byte COMMA = ',';
    public static final byte SEMI_COLON = ';';
    public static final byte COLON = ':';
    public static final byte TAB = '\t';
    public static final byte DOUBLE_QUOTE = '"';

    public static Flow<ByteString, Collection<ByteString>, NotUsed> lineScanner() {
        return lineScanner(COMMA, DOUBLE_QUOTE, BACKSLASH);
    }

    public static Flow<ByteString, Collection<ByteString>, NotUsed> lineScanner(byte delimiter, byte quoteChar, byte escapeChar) {
        return akka.stream.alpakka.csv.scaladsl.CsvParsing
                .lineScanner(delimiter, quoteChar, escapeChar).asJava()
                .map(JavaConversions::asJavaCollection);
    }
}
