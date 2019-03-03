/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.net.InetAddress
import java.util

import com.rabbitmq.client._

/**
 * Pure proxy of a [[Connection]] allowing for easy extension
 * and customization of behaviors for individual methods only.
 *
 * Potentially useful as a spy implementation, this should be
 * as thread-safe as the supplied delegate
 *
 * @param delegate the Connection to proxy methods to if
 *                 otherwise undefined
 */
class AmqpProxyConnection(protected val delegate: Connection) extends Connection {
  override def getAddress: InetAddress = delegate.getAddress()

  override def getPort: Int = delegate.getPort()

  override def getChannelMax: Int = delegate.getChannelMax()

  override def getFrameMax: Int = delegate.getFrameMax()

  override def getHeartbeat: Int = delegate.getHeartbeat()

  override def getClientProperties: util.Map[String, AnyRef] = delegate.getClientProperties()

  override def getClientProvidedName: String = delegate.getClientProvidedName()

  override def getServerProperties: util.Map[String, AnyRef] = delegate.getServerProperties()

  override def createChannel(): Channel = delegate.createChannel()

  override def createChannel(i: Int): Channel = delegate.createChannel(i)

  override def close(): Unit = delegate.close()

  override def close(i: Int, s: String): Unit = delegate.close(i, s)

  override def close(i: Int): Unit = delegate.close(i)

  override def close(i: Int, s: String, i1: Int): Unit = delegate.close(i, s, i1)

  override def abort(): Unit = delegate.abort()

  override def abort(i: Int, s: String): Unit = delegate.abort(i, s)

  override def abort(i: Int): Unit = delegate.abort(i)

  override def abort(i: Int, s: String, i1: Int): Unit = delegate.abort(i, s, i1)

  override def addBlockedListener(blockedListener: BlockedListener): Unit = delegate.addBlockedListener(blockedListener)

  override def addBlockedListener(blockedCallback: BlockedCallback,
                                  unblockedCallback: UnblockedCallback): BlockedListener =
    delegate.addBlockedListener(blockedCallback, unblockedCallback)

  override def removeBlockedListener(blockedListener: BlockedListener): Boolean =
    delegate.removeBlockedListener(blockedListener)

  override def clearBlockedListeners(): Unit = delegate.clearBlockedListeners()

  override def getExceptionHandler: ExceptionHandler = delegate.getExceptionHandler()

  override def getId: String = delegate.getId()

  override def setId(s: String): Unit = delegate.setId(s)

  override def addShutdownListener(shutdownListener: ShutdownListener): Unit =
    delegate.addShutdownListener(shutdownListener)

  override def removeShutdownListener(shutdownListener: ShutdownListener): Unit =
    delegate.removeShutdownListener(shutdownListener)

  override def getCloseReason: ShutdownSignalException = delegate.getCloseReason()

  override def notifyListeners(): Unit = delegate.notifyListeners()

  override def isOpen: Boolean = delegate.isOpen()
}
