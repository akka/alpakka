/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.udp

import java.net.InetSocketAddress

import akka.util.ByteString

final class Datagram private (val data: ByteString, val remote: InetSocketAddress) {

  def withData(data: ByteString) = copy(data = data)

  def withRemote(remote: InetSocketAddress) = copy(remote = remote)

  /**
   * Java API
   */
  def getData(): ByteString = data

  /**
   * Java API
   */
  def getRemote(): InetSocketAddress = remote

  private def copy(data: ByteString = data, remote: InetSocketAddress = remote) =
    new Datagram(data, remote)

  override def toString: String =
    s"""Datagram(
       |  data   = $data
       |  remote = $remote
       |)""".stripMargin
}

object Datagram {
  def apply(data: ByteString, remote: InetSocketAddress) = new Datagram(data, remote)

  /**
   * Java API
   */
  def create(data: ByteString, remote: InetSocketAddress) = Datagram(data, remote)
}
