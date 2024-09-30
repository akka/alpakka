/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.javadsl;

import akka.NotUsed;
import akka.stream.alpakka.csv.scaladsl.CsvQuotingStyle$;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import scala.Option;
import scala.Some;
import scala.jdk.javaapi.CollectionConverters;
import scala.collection.immutable.List;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Optional;

/**
 * Provides CSV formatting flows that convert a sequence of String into their CSV representation in
 * {@see akka.util.ByteString}.
 */
public class CsvFormatting {

  public static final char BACKSLASH = '\\';
  public static final char COMMA = ',';
  public static final char SEMI_COLON = ';';
  public static final char COLON = ':';
  public static final char TAB = '\t';
  public static final char DOUBLE_QUOTE = '"';
  public static final String CR_LF = "\r\n";

  /**
   * Generates standard CSV format (with commas).
   *
   * @param <T> Any collection implementation
   * @return The formatting flow
   */
  public static <T extends Collection<String>> Flow<T, ByteString, NotUsed> format() {
    return format(
        COMMA,
        DOUBLE_QUOTE,
        BACKSLASH,
        CR_LF,
        CsvQuotingStyle.REQUIRED,
        StandardCharsets.UTF_8,
        Optional.empty());
  }

  /**
   * Generates CSV with the specified special characters and character set.
   *
   * @param delimiter Delimiter between columns
   * @param quoteChar Quoting character
   * @param escapeChar Escape character
   * @param endOfLine End of line character sequence
   * @param quotingStyle Quote all values or as required
   * @param charset Character set to be used
   * @param <T> Any collection implementation
   * @return The formatting flow
   */
  public static <T extends Collection<String>> Flow<T, ByteString, NotUsed> format(
      char delimiter,
      char quoteChar,
      char escapeChar,
      String endOfLine,
      CsvQuotingStyle quotingStyle,
      Charset charset,
      Optional<ByteString> byteOrderMark) {
    akka.stream.alpakka.csv.scaladsl.CsvQuotingStyle qs =
        CsvQuotingStyle$.MODULE$.asScala(quotingStyle);
    Option<ByteString> byteOrderMarkScala =
        byteOrderMark.<Option<ByteString>>map(Some::apply).orElse(Option.empty());
    akka.stream.scaladsl.Flow<List<String>, ByteString, NotUsed> formattingFlow =
        akka.stream.alpakka.csv.scaladsl.CsvFormatting.format(
            delimiter, quoteChar, escapeChar, endOfLine, qs, charset, byteOrderMarkScala);
    return Flow.<T>create()
        .map(c -> CollectionConverters.asScala(c).toList())
        .via(formattingFlow);
  }
}
