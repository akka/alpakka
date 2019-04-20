/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket
import java.net.SocketAddress
import java.nio.file.Path

/**
 * Represents a path to a file on a file system that a Unix Domain Socket can
 * bind or connect to.
 */
final case class UnixSocketAddress(path: Path) extends SocketAddress
