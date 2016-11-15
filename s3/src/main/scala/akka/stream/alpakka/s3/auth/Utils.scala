package akka.stream.alpakka.s3.auth

import javax.xml.bind.DatatypeConverter

import akka.util.ByteString

object Utils {
  def encodeHex(bytes: Array[Byte]): String = {
    DatatypeConverter.printHexBinary(bytes).toLowerCase
  }

  def encodeHex(bytes: ByteString): String = {
    encodeHex(bytes.toArray)
  }
}
