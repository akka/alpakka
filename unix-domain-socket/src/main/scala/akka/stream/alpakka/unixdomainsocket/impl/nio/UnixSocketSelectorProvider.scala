/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.impl.nio

import java.net.ProtocolFamily
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.{DatagramChannel, Pipe, ServerSocketChannel, SocketChannel}

import akka.annotation.InternalApi
import akka.stream.alpakka.unixdomainsocket.UnixSocketAddress

/**
 * INTERNAL API
 */
@InternalApi
private[unixdomainsocket] object UnixSocketSelectorProvider extends UnixSocketSelectorProvider

/**
 * INTERNAL API
 */
@InternalApi
private[unixdomainsocket] class UnixSocketSelectorProvider extends SelectorProvider {
  override def openDatagramChannel(): DatagramChannel =
    throw new NotImplementedError

  override def openDatagramChannel(family: ProtocolFamily): DatagramChannel =
    throw new NotImplementedError

  override def openPipe(): Pipe =
    throw new NotImplementedError

  override def openSelector(): UnixSocketSelector =
    new UnixSocketSelector(this)

  override def openServerSocketChannel(): ServerSocketChannel =
    throw new NotImplementedError

  override def openSocketChannel(): SocketChannel =
    throw new NotImplementedError

  def bindAndListenNonBlocked(local: UnixSocketAddress, backlog: Int): UnixServerSocketChannel = {
    val socket = UnixSocket()
    socket.setBlocking(false)
    socket.bind(local)
    socket.listen(backlog)
    new UnixServerSocketChannel(this, socket)
  }

  def connectNonBlocked(remote: UnixSocketAddress): UnixSocketChannel = {
    val socket = UnixSocket()
    socket.setBlocking(false)
    socket.connect(remote)
    new UnixSocketChannel(this, socket)
  }
}
