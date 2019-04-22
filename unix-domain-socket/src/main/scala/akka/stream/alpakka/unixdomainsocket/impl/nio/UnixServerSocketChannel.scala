/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket
package impl.nio

import java.nio.channels.SelectionKey
import java.nio.channels.spi.{AbstractSelectableChannel, SelectorProvider}

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[unixdomainsocket] final class UnixServerSocketChannel(provider: SelectorProvider,
                                                              private val socket: UnixSocket)
    extends AbstractSelectableChannel(provider) {

  override def implCloseSelectableChannel(): Unit =
    socket.close()

  override def implConfigureBlocking(block: Boolean): Unit =
    socket.setBlocking(block)

  override def validOps(): Int =
    SelectionKey.OP_ACCEPT

  def acceptNonBlocked(): UnixSocketChannel = {
    val acceptedSocket = socket.accept()
    acceptedSocket.setBlocking(false)
    new UnixSocketChannel(provider, acceptedSocket)
  }
}
