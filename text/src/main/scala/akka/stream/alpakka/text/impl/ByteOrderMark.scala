package akka.stream.alpakka.text.impl

import akka.util.ByteString

// Todo: this is copied from alpakka-csv - need to decide what to do with it
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
