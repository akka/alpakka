/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package impl

import java.nio.charset.StandardCharsets

import akka.annotation.InternalApi

/*
 * Provides the ability to form valid actor names
 */
@InternalApi object ActorName {
  def mkName(name: String): String =
    name.getBytes(StandardCharsets.UTF_8).map(b => ("0" + b.toHexString).takeRight(2)).mkString
}
