/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.javadsl;

import akka.util.ByteString;

/**
 * Byte Order Marks may be used to indicate the used character encoding in text files.
 *
 * @see <a href="https://www.unicode.org/faq/utf_bom.html#bom1">Unicode FAQ Byte Order Mark</a>
 */
public class ByteOrderMark {
  /** Byte Order Mark for UTF-16 big-endian */
  public static final ByteString UTF_16_BE =
      akka.stream.alpakka.csv.scaladsl.ByteOrderMark.UTF_16_BE();

  /** Byte Order Mark for UTF-16 little-endian */
  public static final ByteString UTF_16_LE =
      akka.stream.alpakka.csv.scaladsl.ByteOrderMark.UTF_16_LE();

  /** Byte Order Mark for UTF-32 big-endian */
  public static final ByteString UTF_32_BE =
      akka.stream.alpakka.csv.scaladsl.ByteOrderMark.UTF_32_BE();

  /** Byte Order Mark for UTF-32 little-endian */
  public static final ByteString UTF_32_LE =
      akka.stream.alpakka.csv.scaladsl.ByteOrderMark.UTF_32_LE();

  /** Byte Order Mark for UTF-8 */
  public static final ByteString UTF_8 = akka.stream.alpakka.csv.scaladsl.ByteOrderMark.UTF_8();
}
