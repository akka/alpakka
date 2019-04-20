/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.impl.nio

import java.nio.ByteBuffer
import java.nio.channels._
import java.nio.channels.spi.{AbstractSelectableChannel, SelectorProvider}

import akka.annotation.InternalApi
import akka.stream.alpakka.unixdomainsocket.UnixSocketAddress

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

  override def validOps(): Int = ???

  override def read(dst: ByteBuffer): Int =
    socket.read(dst)

  override def write(src: ByteBuffer): Int =
    socket.write(src)

  def finishConnect(): Boolean = ???

  def getRemoteAddress: UnixSocketAddress = ???

  def shutdownInput(): Unit = ???

  def shutdownOutput(): Unit = ???
}
