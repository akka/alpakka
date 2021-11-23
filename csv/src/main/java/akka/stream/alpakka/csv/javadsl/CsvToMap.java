/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.javadsl;

import akka.stream.alpakka.csv.impl.CsvToMapAsStringsJavaStage;
import akka.stream.alpakka.csv.impl.CsvToMapJavaStage;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class CsvToMap {

  /**
   * A flow translating incoming {@link Collection<ByteString>} to a {@link Map<String, ByteString>}
   * using the stream's first element's values as keys. The charset to decode [[ByteString]] to
   * [[String]] defaults to UTF-8.
   */
  public static Flow<Collection<ByteString>, Map<String, ByteString>, ?> toMap() {
    return toMap(StandardCharsets.UTF_8);
  }

  /**
   * A flow translating incoming {@link Collection<ByteString>} to a {@link Map<String, ByteString>}
   * using the stream's first element's values as keys.
   *
   * @param charset the charset to decode {@link ByteString} to {@link String}
   */
  public static Flow<Collection<ByteString>, Map<String, ByteString>, ?> toMap(Charset charset) {
    return Flow.fromGraph(
        new CsvToMapJavaStage(
            Optional.empty(), charset, false, Optional.empty(), Optional.empty()));
  }

  /**
   * A flow translating incoming {@link Collection<ByteString>} to a {@link Map<String, ByteString>}
   * using the stream's first element's values as keys.
   *
   * @param charset the charset to decode {@link ByteString} to {@link String}
   */
  public static Flow<Collection<ByteString>, Map<String, String>, ?> toMapAsStrings(
      Charset charset) {
    return Flow.fromGraph(
        new CsvToMapAsStringsJavaStage(
            Optional.empty(), charset, false, Optional.empty(), Optional.empty()));
  }

  /**
   * A flow translating incoming {@link Collection<ByteString>} to a {@link Map<String, ByteString>}
   * using the stream's first element's values as keys. If the header values are shorter than the
   * data (or vice-versa) placeholder elements are used to extend the shorter collection to the
   * length of the longer.
   *
   * @param charset the charset to decode {@link ByteString} to {@link String}, defaults to UTF-8
   * @param customFieldValuePlaceholder placeholder used when there are more data than headers.
   * @param headerPlaceholder placeholder used when there are more headers than data.
   */
  public static Flow<Collection<ByteString>, Map<String, ByteString>, ?> toMapCombineAll(
      Charset charset,
      Optional<ByteString> customFieldValuePlaceholder,
      Optional<String> headerPlaceholder) {
    return Flow.fromGraph(
        new CsvToMapJavaStage(
            Optional.empty(), charset, true, customFieldValuePlaceholder, headerPlaceholder));
  }

  /**
   * A flow translating incoming {@link Collection<ByteString>} to a {@link Map<String, ByteString>}
   * using the stream's first element's values as keys. If the header values are shorter than the
   * data (or vice-versa) placeholder elements are used to extend the shorter collection to the
   * length of the longer.
   *
   * @param charset the charset to decode {@link ByteString} to {@link String}, defaults to UTF-8
   * @param customFieldValuePlaceholder placeholder used when there are more data than headers.
   * @param headerPlaceholder placeholder used when there are more headers than data.
   */
  public static Flow<Collection<ByteString>, Map<String, String>, ?> toMapAsStringsCombineAll(
      Charset charset,
      Optional<String> customFieldValuePlaceholder,
      Optional<String> headerPlaceholder) {
    return Flow.fromGraph(
        new CsvToMapAsStringsJavaStage(
            Optional.empty(), charset, true, customFieldValuePlaceholder, headerPlaceholder));
  }

  /**
   * A flow translating incoming {@link Collection<ByteString>} to a {@link Map<String, ByteString>}
   * using the given headers as keys.
   *
   * @param headers column names to be used as map keys
   */
  public static Flow<Collection<ByteString>, Map<String, ByteString>, ?> withHeaders(
      String... headers) {
    return Flow.fromGraph(
        new CsvToMapJavaStage(
            Optional.of(Arrays.asList(headers)),
            StandardCharsets.UTF_8,
            false,
            Optional.empty(),
            Optional.empty()));
  }

  /**
   * A flow translating incoming {@link Collection<ByteString>} to a {@link Map<String, ByteString>}
   * using the given headers as keys.
   *
   * @param headers column names to be used as map keys
   */
  public static Flow<Collection<ByteString>, Map<String, String>, ?> withHeadersAsStrings(
      Charset charset, String... headers) {
    return Flow.fromGraph(
        new CsvToMapAsStringsJavaStage(
            Optional.of(Arrays.asList(headers)),
            charset,
            false,
            Optional.empty(),
            Optional.empty()));
  }
}
