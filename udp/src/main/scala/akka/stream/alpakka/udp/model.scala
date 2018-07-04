/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.udp

import java.net.InetSocketAddress

import akka.util.ByteString

final case class Datagram(data: ByteString, remote: InetSocketAddress)
object Datagram {

  /**
   * Java API
   */
  def create(data: ByteString, remote: InetSocketAddress) = Datagram(data, remote)
}
