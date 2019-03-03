/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.annotation.InternalApi

/*
 * Provides the ability to form valid actor names
 */
@InternalApi object ActorName {
  private val Utf8 = StandardCharsets.UTF_8.name()

  def mkName(name: String): String =
    URLEncoder.encode(name, Utf8)
}
