/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.ConcurrentModificationException
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
  def release(connection: Connection): Unit = if (connection.isOpen) connection.close()
}

/**
 * Connects to a local AMQP broker at the default port with no password.
 */
case object AmqpLocalConnectionProvider extends AmqpConnectionProvider {
  override def get: Connection = new ConnectionFactory().newConnection

  /**
   * Java API
   */
  def getInstance(): AmqpLocalConnectionProvider.type = this
}

final case class AmqpUriConnectionProvider(uri: String) extends AmqpConnectionProvider {
  override def get: Connection = {
    val factory = new ConnectionFactory
    factory.setUri(uri)
    factory.newConnection
  }
}

object AmqpUriConnectionProvider {

  /**
   * Java API
   */
  def create(uri: String): AmqpUriConnectionProvider = AmqpUriConnectionProvider(uri)
}

final case class AmqpDetailsConnectionProvider(
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

  def withHostsAndPorts(hostAndPort: (String, Int), hostAndPorts: (String, Int)*): AmqpDetailsConnectionProvider =
    copy(hostAndPortList = (hostAndPort +: hostAndPorts).toList)

  def withCredentials(amqpCredentials: AmqpCredentials): AmqpDetailsConnectionProvider =
    copy(credentials = Option(amqpCredentials))

  def withVirtualHost(virtualHost: String): AmqpDetailsConnectionProvider =
    copy(virtualHost = Option(virtualHost))

  def withSslProtocol(sslProtocol: String): AmqpDetailsConnectionProvider =
    copy(sslProtocol = Option(sslProtocol))

  def withRequestedHeartbeat(requestedHeartbeat: Int): AmqpDetailsConnectionProvider =
    copy(requestedHeartbeat = Option(requestedHeartbeat))

  def withConnectionTimeout(connectionTimeout: Int): AmqpDetailsConnectionProvider =
    copy(connectionTimeout = Option(connectionTimeout))

  def withHandshakeTimeout(handshakeTimeout: Int): AmqpDetailsConnectionProvider =
    copy(handshakeTimeout = Option(handshakeTimeout))

  def withShutdownTimeout(shutdownTimeout: Int): AmqpDetailsConnectionProvider =
    copy(shutdownTimeout = Option(shutdownTimeout))

  def withNetworkRecoveryInterval(networkRecoveryInterval: Int): AmqpDetailsConnectionProvider =
    copy(networkRecoveryInterval = Option(networkRecoveryInterval))

  def withAutomaticRecoveryEnabled(automaticRecoveryEnabled: Boolean): AmqpDetailsConnectionProvider =
    copy(automaticRecoveryEnabled = Option(automaticRecoveryEnabled))

  def withTopologyRecoveryEnabled(topologyRecoveryEnabled: Boolean): AmqpDetailsConnectionProvider =
    copy(topologyRecoveryEnabled = Option(topologyRecoveryEnabled))

  def withExceptionHandler(exceptionHandler: ExceptionHandler): AmqpDetailsConnectionProvider =
    copy(exceptionHandler = Option(exceptionHandler))

  /**
   * Java API
   */
  @annotation.varargs
  def withHostsAndPorts(hostAndPort: akka.japi.Pair[String, Int],
                        hostAndPorts: akka.japi.Pair[String, Int]*): AmqpDetailsConnectionProvider =
    copy(hostAndPortList = (hostAndPort +: hostAndPorts).map(_.toScala).toList)

  override def get: Connection = {
    import scala.collection.JavaConverters._
    val factory = new ConnectionFactory
    credentials.foreach { credentials =>
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

object AmqpDetailsConnectionProvider {

  def apply(host: String, port: Int): AmqpDetailsConnectionProvider =
    AmqpDetailsConnectionProvider(List((host, port)))

  /**
   * Java API
   */
  def create(host: String, port: Int): AmqpDetailsConnectionProvider =
    AmqpDetailsConnectionProvider(host, port)
}

final case class AmqpCredentials(username: String, password: String) {
  override def toString = s"Credentials($username, ********)"
}

object AmqpCredentials {

  /**
   * Java API
   */
  def create(username: String, password: String): AmqpCredentials =
    AmqpCredentials(username, password)
}

/**
 * Uses a native [[com.rabbitmq.client.ConnectionFactory]] to configure an AMQP connection factory.
 *
 * @param factory      The instance of the ConnectionFactory to build the connection from.
 * @param hostAndPorts An optional list of host and ports.
 *                     If empty, it defaults to the host and port in the underlying factory.
 */
final case class AmqpConnectionFactoryConnectionProvider(factory: ConnectionFactory,
                                                         private val hostAndPorts: Seq[(String, Int)] = Seq())
    extends AmqpConnectionProvider {

  /**
   * @return A list of hosts and ports for this AMQP connection factory.
   *         Uses host and port from the underlying factory if hostAndPorts was left out on construction.
   */
  def hostAndPortList: Seq[(String, Int)] =
    if (hostAndPorts.isEmpty)
      Seq((factory.getHost, factory.getPort))
    else
      hostAndPorts.toList

  def withHostsAndPorts(hostAndPort: (String, Int),
                        hostAndPorts: (String, Int)*): AmqpConnectionFactoryConnectionProvider =
    copy(hostAndPorts = (hostAndPort +: hostAndPorts).toList)

  /**
   * Java API
   */
  @annotation.varargs
  def withHostsAndPorts(hostAndPort: akka.japi.Pair[String, Int],
                        hostAndPorts: akka.japi.Pair[String, Int]*): AmqpConnectionFactoryConnectionProvider =
    copy(hostAndPorts = (hostAndPort +: hostAndPorts).map(_.toScala).toList)

  override def get: Connection = {
    import scala.collection.JavaConverters._
    factory.newConnection(hostAndPortList.map(hp => new Address(hp._1, hp._2)).asJava)
  }
}

object AmqpConnectionFactoryConnectionProvider {

  /**
   * Java API
   */
  def create(factory: ConnectionFactory): AmqpConnectionFactoryConnectionProvider =
    AmqpConnectionFactoryConnectionProvider(factory: ConnectionFactory)
}

final case class AmqpCachedConnectionProvider(provider: AmqpConnectionProvider, automaticRelease: Boolean = true)
    extends AmqpConnectionProvider {

  import akka.stream.alpakka.amqp.AmqpCachedConnectionProvider._
  private val state = new AtomicReference[State](Empty)

  @tailrec
  override def get: Connection = state.get match {
    case Empty =>
      if (state.compareAndSet(Empty, Connecting)) {
        try {
          val connection = provider.get
          if (!state.compareAndSet(Connecting, Connected(connection, 1)))
            throw new ConcurrentModificationException(
              "Unexpected concurrent modification while creating the connection."
            )
          connection
        } catch {
          case e: ConcurrentModificationException => throw e
          case e: Throwable =>
            state.compareAndSet(Connecting, Empty)
            throw e
        }
      } else get
    case Connecting => get
    case c @ Connected(connection, clients) =>
      if (state.compareAndSet(c, Connected(connection, clients + 1))) connection
      else get
    case Closing => get
  }

  @tailrec
  override def release(connection: Connection): Unit = state.get match {
    case Empty => throw new IllegalStateException("There is no connection to release.")
    case Connecting => release(connection)
    case c @ Connected(cachedConnection, clients) =>
      if (cachedConnection != connection)
        throw new IllegalArgumentException("Can't release a connection that's not owned by this provider")

      if (clients == 1 || !automaticRelease) {
        if (state.compareAndSet(c, Closing)) {
          provider.release(connection)
          if (!state.compareAndSet(Closing, Empty))
            throw new ConcurrentModificationException(
              "Unexpected concurrent modification while closing the connection."
            )
        }
      } else {
        if (!state.compareAndSet(c, Connected(cachedConnection, clients - 1))) release(connection)
      }
    case Closing => release(connection)
  }
}

object AmqpCachedConnectionProvider {

  /**
   * Java API
   */
  def create(provider: AmqpConnectionProvider): AmqpCachedConnectionProvider =
    AmqpCachedConnectionProvider(provider: AmqpConnectionProvider)

  /**
   * Java API
   */
  def create(provider: AmqpConnectionProvider, automaticRelease: Boolean): AmqpCachedConnectionProvider =
    AmqpCachedConnectionProvider(provider: AmqpConnectionProvider, automaticRelease)

  private sealed trait State
  private case object Empty extends State
  private case object Connecting extends State
  private final case class Connected(connection: Connection, clients: Int) extends State
  private case object Closing extends State
}
