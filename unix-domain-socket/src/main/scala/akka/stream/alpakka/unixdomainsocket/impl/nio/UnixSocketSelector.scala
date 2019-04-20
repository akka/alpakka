/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.impl.nio
import java.nio.channels.{SelectionKey, Selector}
import java.nio.channels.spi.{AbstractSelectableChannel, AbstractSelector, SelectorProvider}
import java.util

import akka.annotation.InternalApi

/**
 * INTERNAL API
 *
 * This class is the centre of the UDS universe in that it calls on the OS to
 * receive notifications w.r.t. when IO is signalled on the file descriptors
 * we manage. This information is accordingly passed on to NIO.
 */
@InternalApi
private[unixdomainsocket] final class UnixSocketSelector(provider: SelectorProvider)
    extends AbstractSelector(provider) {
  override def implCloseSelector(): Unit = ???
  override def register(ch: AbstractSelectableChannel, ops: Int, att: Any): SelectionKey = ???
  override def keys(): util.Set[SelectionKey] = ???
  override def selectedKeys(): util.Set[SelectionKey] = ???
  override def selectNow(): Int = ???
  override def select(timeout: Long): Int = ???
  override def select(): Int = ???
  override def wakeup(): Selector = ???
}
