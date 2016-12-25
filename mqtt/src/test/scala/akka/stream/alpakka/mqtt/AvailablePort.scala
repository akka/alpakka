/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.mqtt

import java.net.ServerSocket

trait AvailablePort {

  def nextPort(): Int = this.synchronized {
    val socket = new ServerSocket(0)
    socket.close()
    socket.getLocalPort
  }
}

object AvailablePort extends AvailablePort
