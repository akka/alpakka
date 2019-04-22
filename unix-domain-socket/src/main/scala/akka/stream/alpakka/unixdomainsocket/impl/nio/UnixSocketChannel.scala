/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.impl.nio

import java.nio.ByteBuffer
import java.nio.channels._
import java.nio.channels.spi.{AbstractSelectableChannel, SelectorProvider}

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[unixdomainsocket] final class UnixSocketChannel(provider: SelectorProvider, private val socket: UnixSocket)
    extends AbstractSelectableChannel(provider)
    with ByteChannel {

  override def implCloseSelectableChannel(): Unit =
    socket.close()

  override def implConfigureBlocking(block: Boolean): Unit =
    socket.setBlocking(block)

  override def validOps(): Int =
    SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT

  override def read(dst: ByteBuffer): Int =
    socket.read(dst)

  override def write(src: ByteBuffer): Int =
    socket.write(src)

  def shutdownInput(): Unit =
    socket.shutdownInput()

  def shutdownOutput(): Unit =
    socket.shutdownOutput()
}
