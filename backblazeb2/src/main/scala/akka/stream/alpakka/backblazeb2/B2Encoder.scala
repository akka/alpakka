/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util.Base64

object B2Encoder {
  def encode(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8.name).replace("%2F", "/")

  def decode(s: String): String =
    URLDecoder.decode(s, StandardCharsets.UTF_8.name)

  def encodeBase64(s: String): String = {
    val encoded = Base64.getEncoder.encode(s.getBytes(StandardCharsets.UTF_8))
    new String(encoded, StandardCharsets.UTF_8.name)
  }
}
