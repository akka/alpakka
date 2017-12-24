package akka.stream.alpakka.amqp

import com.rabbitmq.client.{Address, Connection, ConnectionFactory, ExceptionHandler}


/**
  * Only for internal implementations
  */
sealed trait AmqpConnectionSettings {
  private[amqp] def getConnection: Connection
}

/**
  * Connects to a local AMQP broker at the default port with no password.
  */
case class AmqpConnectionLocal() extends AmqpConnectionSettings {
  override def getConnection: Connection = new ConnectionFactory().newConnection
}

object AmqpConnectionLocal {

  /**
    * Java API
    */
  def create(): AmqpConnectionLocal = AmqpConnectionLocal()
}

/**
  * Connects to a local AMQP broker at the default port with no password.
  */
case object DefaultAmqpConnection extends AmqpConnectionSettings {

  /**
    * Java API
    */
  def getInstance(): DefaultAmqpConnection.type = this

  override def getConnection: Connection = new ConnectionFactory().newConnection
}

final case class AmqpConnectionUri(uri: String) extends AmqpConnectionSettings {
  override def getConnection: Connection = {
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
                                      ) extends AmqpConnectionSettings {

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

  override def getConnection: Connection = {
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