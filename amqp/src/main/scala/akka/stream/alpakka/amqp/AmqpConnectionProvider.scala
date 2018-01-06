/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.concurrent.atomic.AtomicReference

import akka.annotation.DoNotInherit
import com.rabbitmq.client.{Address, Connection, ConnectionFactory, ExceptionHandler}

import scala.annotation.tailrec

/**
 * Only for internal implementations
 */
@DoNotInherit
sealed trait AmqpConnectionProvider {
  def get: Connection
  def release(connection: Connection): Unit = connection.close()
}

/**
 * Connects to a local AMQP broker at the default port with no password.
 */
case object AmqpConnectionLocal extends AmqpConnectionProvider {
  override def get: Connection = new ConnectionFactory().newConnection

  /**
   * Java API
   */
  def getInstance(): AmqpConnectionLocal.type = this
}

final case class AmqpConnectionUri(uri: String) extends AmqpConnectionProvider {
  override def get: Connection = {
    val factory = new ConnectionFactory
    factory.setUri(uri)
    factory.newConnection
  }
}

object AmqpConnectionUri {

  /**
   * Java API
   */
  def create(uri: String): AmqpConnectionUri = AmqpConnectionUri(uri)
}

final case class AmqpConnectionDetails(
    hostAndPortList: Seq[(String, Int)],
    credentials: Option[AmqpCredentials] = None,
    virtualHost: Option[String] = None,
    sslProtocol: Option[String] = None,
    requestedHeartbeat: Option[Int] = None,
    connectionTimeout: Option[Int] = None,
    handshakeTimeout: Option[Int] = None,
    shutdownTimeout: Option[Int] = None,
    networkRecoveryInterval: Option[Int] = None,
    automaticRecoveryEnabled: Option[Boolean] = None,
    topologyRecoveryEnabled: Option[Boolean] = None,
    exceptionHandler: Option[ExceptionHandler] = None
) extends AmqpConnectionProvider {

  def withHostsAndPorts(hostAndPort: (String, Int), hostAndPorts: (String, Int)*): AmqpConnectionDetails =
    copy(hostAndPortList = (hostAndPort +: hostAndPorts).toList)

  def withCredentials(amqpCredentials: AmqpCredentials): AmqpConnectionDetails =
    copy(credentials = Option(amqpCredentials))

  def withVirtualHost(virtualHost: String): AmqpConnectionDetails =
    copy(virtualHost = Option(virtualHost))

  def withSslProtocol(sslProtocol: String): AmqpConnectionDetails =
    copy(sslProtocol = Option(sslProtocol))

  def withRequestedHeartbeat(requestedHeartbeat: Int): AmqpConnectionDetails =
    copy(requestedHeartbeat = Option(requestedHeartbeat))

  def withConnectionTimeout(connectionTimeout: Int): AmqpConnectionDetails =
    copy(connectionTimeout = Option(connectionTimeout))

  def withHandshakeTimeout(handshakeTimeout: Int): AmqpConnectionDetails =
    copy(handshakeTimeout = Option(handshakeTimeout))

  def withShutdownTimeout(shutdownTimeout: Int): AmqpConnectionDetails =
    copy(shutdownTimeout = Option(shutdownTimeout))

  def withNetworkRecoveryInterval(networkRecoveryInterval: Int): AmqpConnectionDetails =
    copy(networkRecoveryInterval = Option(networkRecoveryInterval))

  def withAutomaticRecoveryEnabled(automaticRecoveryEnabled: Boolean): AmqpConnectionDetails =
    copy(automaticRecoveryEnabled = Option(automaticRecoveryEnabled))

  def withTopologyRecoveryEnabled(topologyRecoveryEnabled: Boolean): AmqpConnectionDetails =
    copy(topologyRecoveryEnabled = Option(topologyRecoveryEnabled))

  def withExceptionHandler(exceptionHandler: ExceptionHandler): AmqpConnectionDetails =
    copy(exceptionHandler = Option(exceptionHandler))

  /**
   * Java API
   */
  @annotation.varargs
  def withHostsAndPorts(hostAndPort: akka.japi.Pair[String, Int],
                        hostAndPorts: akka.japi.Pair[String, Int]*): AmqpConnectionDetails =
    copy(hostAndPortList = (hostAndPort +: hostAndPorts).map(_.toScala).toList)

  override def get: Connection = {
    import scala.collection.JavaConverters._
    val factory = new ConnectionFactory
    credentials.foreach { credentials =>
      val factory = new ConnectionFactory
      factory.setUsername(credentials.username)
      factory.setPassword(credentials.password)
    }
    virtualHost.foreach(factory.setVirtualHost)
    sslProtocol.foreach(factory.useSslProtocol)
    requestedHeartbeat.foreach(factory.setRequestedHeartbeat)
    connectionTimeout.foreach(factory.setConnectionTimeout)
    handshakeTimeout.foreach(factory.setHandshakeTimeout)
    shutdownTimeout.foreach(factory.setShutdownTimeout)
    networkRecoveryInterval.foreach(factory.setNetworkRecoveryInterval)
    automaticRecoveryEnabled.foreach(factory.setAutomaticRecoveryEnabled)
    topologyRecoveryEnabled.foreach(factory.setTopologyRecoveryEnabled)
    exceptionHandler.foreach(factory.setExceptionHandler)

    factory.newConnection(hostAndPortList.map(hp => new Address(hp._1, hp._2)).asJava)
  }

}

object AmqpConnectionDetails {

  def apply(host: String, port: Int): AmqpConnectionDetails =
    AmqpConnectionDetails(List((host, port)))

  /**
   * Java API
   */
  def create(host: String, port: Int): AmqpConnectionDetails =
    AmqpConnectionDetails(host, port)
}

object AmqpCredentials {

  /**
   * Java API
   */
  def create(username: String, password: String): AmqpCredentials =
    AmqpCredentials(username, password)
}

final case class AmqpCredentials(username: String, password: String) {
  override def toString = s"Credentials($username, ********)"
}

object ReusableAmqpConnectionProvider {

  /**
   * Java API
   */
  def create(provider: AmqpConnectionProvider): ReusableAmqpConnectionProvider =
    ReusableAmqpConnectionProvider(provider: AmqpConnectionProvider)

  /**
   * Java API
   */
  def create(provider: AmqpConnectionProvider, automaticRelease: Boolean): ReusableAmqpConnectionProvider =
    ReusableAmqpConnectionProvider(provider: AmqpConnectionProvider, automaticRelease)

  private sealed trait State
  private case object Empty extends State
  private case object Connecting extends State
  private final case class Connected(connection: Connection, clients: Int) extends State
  private case object Closing extends State
}

final case class ReusableAmqpConnectionProvider(provider: AmqpConnectionProvider, automaticRelease: Boolean = true)
    extends AmqpConnectionProvider {

  import akka.stream.alpakka.amqp.ReusableAmqpConnectionProvider._
  private val state = new AtomicReference[State](Empty)

  @tailrec
  override def get: Connection = state.get match {
    case Empty =>
      if (state.compareAndSet(Empty, Connecting)) {
        val connection = provider.get
        state.compareAndSet(Connecting, Connected(connection, 0))
        connection
      } else get
    case c @ Connected(connection, clients) =>
      if (state.compareAndSet(c, Connected(connection, clients + 1))) connection
      else get
    case _ => get
  }

  @tailrec
  override def release(connection: Connection): Unit = state.get match {
    case c @ Connected(cachedConnection, clients) =>
      if (state.compareAndSet(c, Closing)) {
        if (cachedConnection != connection)
          throw new IllegalArgumentException("Can't release a connection that's not owned by this provider")
        else if (clients == 0 || !automaticRelease) {
          provider.release(connection)
          state.compareAndSet(Closing, Empty)
        } else {
          state.compareAndSet(Closing, Connected(cachedConnection, clients - 1))
        }
      } else release(connection)
    case _ => release(connection)
  }
}
