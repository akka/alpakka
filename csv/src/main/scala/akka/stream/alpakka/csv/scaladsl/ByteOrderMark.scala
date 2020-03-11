/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.scaladsl

import akka.util.ByteString

/**
 * Byte Order Marks may be used to indicate the used character encoding
 * in text files.
 *
 * @see http://www.unicode.org/faq/utf_bom.html#bom1
 */
object ByteOrderMark {

  private[this] final val ZeroZero = ByteString.apply(0x00.toByte, 0x00.toByte)

  /** Byte Order Mark for UTF-16 big-endian */
  final val UTF_16_BE = ByteString.apply(0xFE.toByte, 0xFF.toByte)

  /** Byte Order Mark for UTF-16 little-endian */
  final val UTF_16_LE = ByteString.apply(0xFF.toByte, 0xFE.toByte)

  /** Byte Order Mark for UTF-32 big-endian */
  final val UTF_32_BE = ZeroZero ++ UTF_16_BE

  /** Byte Order Mark for UTF-32 little-endian */
  final val UTF_32_LE = UTF_16_LE ++ ZeroZero

  /** Byte Order Mark for UTF-8 */
  final val UTF_8 = ByteString.apply(0xEF.toByte, 0xBB.toByte, 0xBF.toByte)
}
