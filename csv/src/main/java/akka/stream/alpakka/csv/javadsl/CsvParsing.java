/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import scala.collection.JavaConverters;

import java.util.Collection;

public class CsvParsing {

  public static final byte BACKSLASH = '\\';
  public static final byte COMMA = ',';
  public static final byte SEMI_COLON = ';';
  public static final byte COLON = ':';
  public static final byte TAB = '\t';
  public static final byte DOUBLE_QUOTE = '"';
  public static final int MAXIMUM_LINE_LENGTH_DEFAULT = 10 * 1024;

  public static Flow<ByteString, Collection<ByteString>, NotUsed> lineScanner() {
    return lineScanner(COMMA, DOUBLE_QUOTE, BACKSLASH, MAXIMUM_LINE_LENGTH_DEFAULT);
  }

  public static Flow<ByteString, Collection<ByteString>, NotUsed> lineScanner(
      byte delimiter, byte quoteChar, byte escapeChar) {
    return lineScanner(delimiter, quoteChar, escapeChar, MAXIMUM_LINE_LENGTH_DEFAULT);
  }

  public static Flow<ByteString, Collection<ByteString>, NotUsed> lineScanner(
      byte delimiter, byte quoteChar, byte escapeChar, int maximumLineLength) {
    return akka.stream.alpakka.csv.scaladsl.CsvParsing.lineScanner(
            delimiter, quoteChar, escapeChar, maximumLineLength)
        .asJava()
        .map(c -> JavaConverters.asJavaCollectionConverter(c).asJavaCollection())
        .mapMaterializedValue(m -> NotUsed.getInstance());
  }
}
