/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference

import akka.annotation.DoNotInherit
import com.rabbitmq.client.{Address, Connection, ConnectionFactory, ExceptionHandler}
import javax.net.ssl.{SSLContext, TrustManager}

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.JavaConverters._

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

final class AmqpUriConnectionProvider private (val uri: String) extends AmqpConnectionProvider {
  override def get: Connection = {
    val factory = new ConnectionFactory
    factory.setUri(uri)
    factory.newConnection
  }

  override def toString: String =
    "AmqpUriConnectionProvider(" +
    s"uri=$uri" +
    ")"
}

object AmqpUriConnectionProvider {

  def apply(uri: String): AmqpUriConnectionProvider = new AmqpUriConnectionProvider(uri)

  /**
   * Java API
   */
  def create(uri: String): AmqpUriConnectionProvider = AmqpUriConnectionProvider(uri)
}

final class AmqpDetailsConnectionProvider private (
    val hostAndPortList: immutable.Seq[(String, Int)],
    val credentials: Option[AmqpCredentials] = None,
    val virtualHost: Option[String] = None,
    val sslConfiguration: Option[AmqpSSLConfiguration] = None,
    val requestedHeartbeat: Option[Int] = None,
    val connectionTimeout: Option[Int] = None,
    val handshakeTimeout: Option[Int] = None,
    val shutdownTimeout: Option[Int] = None,
    val networkRecoveryInterval: Option[Int] = None,
    val automaticRecoveryEnabled: Boolean = false,
    val topologyRecoveryEnabled: Boolean = false,
    val exceptionHandler: Option[ExceptionHandler] = None,
    val connectionName: Option[String] = None
) extends AmqpConnectionProvider {

  def withHostAndPort(host: String, port: Int): AmqpDetailsConnectionProvider =
    copy(hostAndPortList = immutable.Seq(host -> port))

  def withHostsAndPorts(hostAndPorts: immutable.Seq[(String, Int)]): AmqpDetailsConnectionProvider =
    copy(hostAndPortList = hostAndPorts)

  def withHostsAndPorts(hostAndPorts: java.util.List[akka.japi.Pair[String, Int]]): AmqpDetailsConnectionProvider =
    copy(hostAndPortList = hostAndPorts.asScala.map(_.toScala).toIndexedSeq)

  def withCredentials(amqpCredentials: AmqpCredentials): AmqpDetailsConnectionProvider =
    copy(credentials = Option(amqpCredentials))

  def withVirtualHost(virtualHost: String): AmqpDetailsConnectionProvider =
    copy(virtualHost = Option(virtualHost))

  def withSSLConfiguration(sslConfiguration: AmqpSSLConfiguration): AmqpDetailsConnectionProvider =
    copy(sslConfiguration = Option(sslConfiguration))

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
    copy(automaticRecoveryEnabled = automaticRecoveryEnabled)

  def withTopologyRecoveryEnabled(topologyRecoveryEnabled: Boolean): AmqpDetailsConnectionProvider =
    copy(topologyRecoveryEnabled = topologyRecoveryEnabled)

  def withExceptionHandler(exceptionHandler: ExceptionHandler): AmqpDetailsConnectionProvider =
    copy(exceptionHandler = Option(exceptionHandler))

  def withConnectionName(name: String): AmqpDetailsConnectionProvider =
    copy(connectionName = Option(name))

  override def get: Connection = {
    import scala.collection.JavaConverters._
    val factory = new ConnectionFactory
    credentials.foreach { credentials =>
      factory.setUsername(credentials.username)
      factory.setPassword(credentials.password)
    }
    virtualHost.foreach(factory.setVirtualHost)
    sslConfiguration.foreach(sslConfiguration => {
      if (sslConfiguration.protocol.isDefined) {
        if (sslConfiguration.trustManager.isDefined)
          factory.useSslProtocol(sslConfiguration.protocol.get, sslConfiguration.trustManager.get)
        else factory.useSslProtocol(sslConfiguration.protocol.get)
      } else if (sslConfiguration.context.isDefined) {
        factory.useSslProtocol(sslConfiguration.context.get)
      } else {
        factory.useSslProtocol()
      }
    })
    requestedHeartbeat.foreach(factory.setRequestedHeartbeat)
    connectionTimeout.foreach(factory.setConnectionTimeout)
    handshakeTimeout.foreach(factory.setHandshakeTimeout)
    shutdownTimeout.foreach(factory.setShutdownTimeout)
    networkRecoveryInterval.foreach(factory.setNetworkRecoveryInterval)
    factory.setAutomaticRecoveryEnabled(automaticRecoveryEnabled)
    factory.setTopologyRecoveryEnabled(topologyRecoveryEnabled)
    exceptionHandler.foreach(factory.setExceptionHandler)

    factory.newConnection(hostAndPortList.map(hp => new Address(hp._1, hp._2)).asJava, connectionName.orNull)
  }

  private def copy(hostAndPortList: immutable.Seq[(String, Int)] = hostAndPortList,
                   credentials: Option[AmqpCredentials] = credentials,
                   virtualHost: Option[String] = virtualHost,
                   sslConfiguration: Option[AmqpSSLConfiguration] = sslConfiguration,
                   requestedHeartbeat: Option[Int] = requestedHeartbeat,
                   connectionTimeout: Option[Int] = connectionTimeout,
                   handshakeTimeout: Option[Int] = handshakeTimeout,
                   shutdownTimeout: Option[Int] = shutdownTimeout,
                   networkRecoveryInterval: Option[Int] = networkRecoveryInterval,
                   automaticRecoveryEnabled: Boolean = automaticRecoveryEnabled,
                   topologyRecoveryEnabled: Boolean = topologyRecoveryEnabled,
                   exceptionHandler: Option[ExceptionHandler] = exceptionHandler,
                   connectionName: Option[String] = connectionName): AmqpDetailsConnectionProvider =
    new AmqpDetailsConnectionProvider(
      hostAndPortList,
      credentials,
      virtualHost,
      sslConfiguration,
      requestedHeartbeat,
      connectionTimeout,
      handshakeTimeout,
      shutdownTimeout,
      networkRecoveryInterval,
      automaticRecoveryEnabled,
      topologyRecoveryEnabled,
      exceptionHandler,
      connectionName
    )

  override def toString: String =
    "AmqpDetailsConnectionProvider(" +
    s"hostAndPortList=$hostAndPortList, " +
    s"credentials=$credentials, " +
    s"virtualHost=$virtualHost, " +
    s"sslConfiguration=$sslConfiguration, " +
    s"requestedHeartbeat=$requestedHeartbeat, " +
    s"connectionTimeout=$connectionTimeout, " +
    s"handshakeTimeout=$handshakeTimeout, " +
    s"shutdownTimeout=$shutdownTimeout, " +
    s"networkRecoveryInterval=$networkRecoveryInterval, " +
    s"automaticRecoveryEnabled=$automaticRecoveryEnabled, " +
    s"topologyRecoveryEnabled=$topologyRecoveryEnabled, " +
    s"exceptionHandler=$exceptionHandler, " +
    s"connectionName=$connectionName" +
    ")"
}

object AmqpDetailsConnectionProvider {

  def apply(host: String, port: Int): AmqpDetailsConnectionProvider =
    new AmqpDetailsConnectionProvider(immutable.Seq(host -> port))

  def apply(hostAndPorts: immutable.Seq[(String, Int)]): AmqpDetailsConnectionProvider =
    new AmqpDetailsConnectionProvider(hostAndPorts)

  /**
   * Java API
   */
  def create(host: String, port: Int): AmqpDetailsConnectionProvider =
    AmqpDetailsConnectionProvider(host, port)

  def create(hostAndPorts: java.util.List[akka.japi.Pair[String, Int]]): AmqpDetailsConnectionProvider =
    AmqpDetailsConnectionProvider(hostAndPorts)
}

final class AmqpCredentials private (val username: String, val password: String) {
  override def toString = s"Credentials($username, ********)"
}

object AmqpCredentials {

  def apply(username: String, password: String): AmqpCredentials =
    new AmqpCredentials(username, password)

  /**
   * Java API
   */
  def create(username: String, password: String): AmqpCredentials =
    AmqpCredentials(username, password)
}

final class AmqpSSLConfiguration private (val protocol: Option[String] = None,
                                          val trustManager: Option[TrustManager] = None,
                                          val context: Option[SSLContext] = None) {
  if (protocol.isDefined && context.isDefined) {
    throw new IllegalArgumentException("Protocol and context can't be defined in the same AmqpSSLConfiguration.")
  }

  def withProtocol(protocol: String): AmqpSSLConfiguration =
    copy(protocol = Some(protocol))

  def withProtocolAndTrustManager(protocol: String, trustManager: javax.net.ssl.TrustManager): AmqpSSLConfiguration =
    copy(protocol = Some(protocol), trustManager = Some(trustManager))

  def withSSLContext(context: Option[javax.net.ssl.SSLContext]): AmqpSSLConfiguration =
    copy(context = context)

  private def copy(protocol: Option[String] = protocol,
                   trustManager: Option[TrustManager] = trustManager,
                   context: Option[SSLContext] = context): AmqpSSLConfiguration =
    new AmqpSSLConfiguration(protocol, trustManager, context)

  override def toString: String =
    if (protocol.isDefined) {
      if (trustManager.isDefined) s"AmqpSSLConfiguration(protocol=${protocol.get}, trustManager=${trustManager.get})"
      else s"AmqpSSLConfiguration(protocol=${protocol.get})"
    } else if (context.isDefined) {
      s"AmqpSSLConfiguration(context=${context.get})"
    } else {
      "AmqpSSLConfiguration()"
    }
}

object AmqpSSLConfiguration {

  def apply(protocol: String): AmqpSSLConfiguration = new AmqpSSLConfiguration(Some(protocol))
  def apply(protocol: String, trustManager: TrustManager): AmqpSSLConfiguration =
    new AmqpSSLConfiguration(Some(protocol), Some(trustManager))
  def apply(context: SSLContext): AmqpSSLConfiguration = new AmqpSSLConfiguration(context = Some(context))

  /**
   * Java API
   */
  def create(protocol: String): AmqpSSLConfiguration = new AmqpSSLConfiguration(Some(protocol))

  /**
   * Java API
   */
  def create(protocol: String, trustManager: TrustManager): AmqpSSLConfiguration =
    new AmqpSSLConfiguration(Some(protocol), Some(trustManager))

  /**
   * Java API
   */
  def create(context: SSLContext): AmqpSSLConfiguration = new AmqpSSLConfiguration(context = Some(context))
}

/**
 * Uses a native [[com.rabbitmq.client.ConnectionFactory]] to configure an AMQP connection factory.
 *
 * @param factory      The instance of the ConnectionFactory to build the connection from.
 * @param hostAndPorts An optional list of host and ports.
 *                     If empty, it defaults to the host and port in the underlying factory.
 */
final class AmqpConnectionFactoryConnectionProvider private (val factory: ConnectionFactory,
                                                             private val hostAndPorts: immutable.Seq[(String, Int)] =
                                                               Nil)
    extends AmqpConnectionProvider {

  /**
   * @return A list of hosts and ports for this AMQP connection factory.
   *         Uses host and port from the underlying factory if hostAndPorts was left out on construction.
   */
  def hostAndPortList: immutable.Seq[(String, Int)] =
    if (hostAndPorts.isEmpty)
      immutable.Seq((factory.getHost, factory.getPort))
    else
      hostAndPorts.toList

  def withHostAndPort(host: String, port: Int): AmqpConnectionFactoryConnectionProvider =
    copy(hostAndPorts = immutable.Seq(host -> port))

  def withHostsAndPorts(hostAndPorts: immutable.Seq[(String, Int)]): AmqpConnectionFactoryConnectionProvider =
    copy(hostAndPorts = hostAndPorts)

  /**
   * Java API
   */
  def withHostsAndPorts(
      hostAndPorts: java.util.List[akka.japi.Pair[String, Int]]
  ): AmqpConnectionFactoryConnectionProvider =
    copy(hostAndPorts = hostAndPorts.asScala.map(_.toScala).toIndexedSeq)

  override def get: Connection = {
    import scala.collection.JavaConverters._
    factory.newConnection(hostAndPortList.map(hp => new Address(hp._1, hp._2)).asJava)
  }

  private def copy(hostAndPorts: immutable.Seq[(String, Int)]) =
    new AmqpConnectionFactoryConnectionProvider(factory, hostAndPorts)

  override def toString: String =
    "AmqpConnectionFactoryConnectionProvider(" +
    s"factory=$factory, " +
    s"hostAndPorts=$hostAndPorts" +
    ")"
}

object AmqpConnectionFactoryConnectionProvider {

  def apply(factory: ConnectionFactory): AmqpConnectionFactoryConnectionProvider =
    new AmqpConnectionFactoryConnectionProvider(factory)

  /**
   * Java API
   */
  def create(factory: ConnectionFactory): AmqpConnectionFactoryConnectionProvider =
    AmqpConnectionFactoryConnectionProvider(factory: ConnectionFactory)
}

final class AmqpCachedConnectionProvider private (val provider: AmqpConnectionProvider,
                                                  val automaticRelease: Boolean = true)
    extends AmqpConnectionProvider {

  import akka.stream.alpakka.amqp.AmqpCachedConnectionProvider._
  private val state = new AtomicReference[State](Empty)

  def withAutomaticRelease(automaticRelease: Boolean): AmqpCachedConnectionProvider =
    copy(automaticRelease = automaticRelease)

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

  private def copy(automaticRelease: Boolean): AmqpCachedConnectionProvider =
    new AmqpCachedConnectionProvider(provider, automaticRelease)

  override def toString: String =
    "AmqpCachedConnectionProvider(" +
    s"provider=$provider, " +
    s"automaticRelease=$automaticRelease" +
    ")"
}

object AmqpCachedConnectionProvider {

  def apply(provider: AmqpConnectionProvider): AmqpCachedConnectionProvider =
    new AmqpCachedConnectionProvider(provider)

  /**
   * Java API
   */
  def create(provider: AmqpConnectionProvider): AmqpCachedConnectionProvider =
    AmqpCachedConnectionProvider(provider: AmqpConnectionProvider)

  private sealed trait State
  private case object Empty extends State
  private case object Connecting extends State
  private final case class Connected(connection: Connection, clients: Int) extends State
  private case object Closing extends State
}
