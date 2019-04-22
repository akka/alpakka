/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.impl.nio

import java.io.IOException
import java.nio.ByteBuffer

import akka.annotation.InternalApi
import akka.stream.alpakka.unixdomainsocket.UnixSocketAddress
import com.facebook.nailgun.NGUnixDomainSocketLibrary
import com.sun.jna.LastErrorException
import com.sun.jna.ptr.IntByReference

/**
 * INTERNAL API
 */
@InternalApi
private[unixdomainsocket] object UnixSocket {
  def apply(): UnixSocket =
    new UnixSocket(
      tryIo(
        NGUnixDomainSocketLibrary.socket(NGUnixDomainSocketLibrary.AF_LOCAL, NGUnixDomainSocketLibrary.SOCK_STREAM, 0)
      )
    )

  private def tryIo[A](block: => A): A =
    try {
      block
    } catch {
      case e: LastErrorException => throw new IOException(e)
    }
}

/**
 * INTERNAL API
 *
 * The motivation for this class is to maintain a file descriptor
 * to the native calls. This file descriptor provides complete
 * access to the state managed by the OS w.r.t. a socket. Both
 * the socket and server socket channels utilize instances of
 * this class.
 */
@InternalApi
private[unixdomainsocket] final class UnixSocket(private val fd: Int) {

  import UnixSocket._

  def connect(address: UnixSocketAddress): Unit = {
    val sockAddr = new NGUnixDomainSocketLibrary.SockaddrUn(address.toString)
    try {
      NGUnixDomainSocketLibrary.connect(fd, sockAddr, sockAddr.size)
    } catch {
      case e: LastErrorException if e.getErrorCode == NGUnixDomainSocketLibrary.EINPROGRESS => ()
      case e: LastErrorException => throw new IOException(e)
    }
  }

  def bind(address: UnixSocketAddress): Unit = {
    val sockAddr = new NGUnixDomainSocketLibrary.SockaddrUn(address.toString)
    tryIo(NGUnixDomainSocketLibrary.bind(fd, sockAddr, sockAddr.size))
  }

  def listen(backlog: Int): Unit =
    tryIo(NGUnixDomainSocketLibrary.listen(fd, backlog))

  def accept(): UnixSocket = {
    val sockAddr = new NGUnixDomainSocketLibrary.SockaddrUn
    val sockAddrSize = new IntByReference(sockAddr.size)
    new UnixSocket(tryIo(NGUnixDomainSocketLibrary.accept(fd, sockAddr, sockAddrSize)))
  }

  def read(dst: ByteBuffer): Int =
    tryIo(NGUnixDomainSocketLibrary.read(fd, dst, dst.remaining()))

  def write(src: ByteBuffer): Int =
    tryIo(NGUnixDomainSocketLibrary.write(fd, src, src.remaining()))

  def close(): Unit =
    tryIo(NGUnixDomainSocketLibrary.close(fd))

  def isBlocking: Boolean = {
    val status = tryIo(NGUnixDomainSocketLibrary.fcntl(fd, NGUnixDomainSocketLibrary.F_GETFL, 0))
    (status & NGUnixDomainSocketLibrary.O_NONBLOCK) == NGUnixDomainSocketLibrary.O_NONBLOCK
  }

  def setBlocking(block: Boolean): Unit = {
    val status = tryIo(NGUnixDomainSocketLibrary.fcntl(fd, NGUnixDomainSocketLibrary.F_GETFL, 0))
    tryIo(
      NGUnixDomainSocketLibrary
        .fcntl(fd, NGUnixDomainSocketLibrary.F_SETFL, status | (if (block) NGUnixDomainSocketLibrary.O_NONBLOCK else 0))
    )
  }

  def shutdownInput(): Unit =
    tryIo(NGUnixDomainSocketLibrary.shutdown(fd, NGUnixDomainSocketLibrary.SHUT_RD))

  def shutdownOutput(): Unit =
    tryIo(NGUnixDomainSocketLibrary.shutdown(fd, NGUnixDomainSocketLibrary.SHUT_WR))
}
