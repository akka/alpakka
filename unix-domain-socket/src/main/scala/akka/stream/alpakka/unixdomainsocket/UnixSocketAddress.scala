/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket

import java.net.SocketAddress
import java.nio.file.Path

import akka.annotation.InternalApi

object UnixSocketAddress {

  /**
   * Creates an address representing a Unix Domain Socket
   * @param path the path representing the socket
   * @return the address
   */
  def apply(path: Path): UnixSocketAddress =
    new UnixSocketAddress(path)

  /**
   * Java API:
   * Creates an address representing a Unix Domain Socket
   * @param path the path representing the socket
   * @return the address
   */
  def create(path: Path): UnixSocketAddress =
    new UnixSocketAddress(path)
}

/**
 * Represents a path to a file on a file system that a Unix Domain Socket can
 * bind or connect to.
 */
final class UnixSocketAddress @InternalApi private[unixdomainsocket] (val path: Path) extends SocketAddress
