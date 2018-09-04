/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import akka.util.JavaDurationConverters._
import org.eclipse.paho.client.mqttv3.{MqttClientPersistence, MqttConnectOptions}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.immutable.Map
import scala.concurrent.duration.{FiniteDuration, _}

sealed abstract class MqttQoS {
  def value: Int
}

/**
 * Quality of Service constants as defined in
 * See http://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttMessage.html#setQos-int-
 */
object MqttQoS {
  object AtMostOnce extends MqttQoS {
    val value: Int = 0
  }

  object AtLeastOnce extends MqttQoS {
    val value: Int = 1
  }

  object ExactlyOnce extends MqttQoS {
    val value: Int = 2
  }

  /**
   * Java API
   */
  def atMostOnce: MqttQoS = AtMostOnce

  /**
   * Java API
   */
  def atLeastOnce: MqttQoS = AtLeastOnce

  /**
   * Java API
   */
  def exactlyOnce: MqttQoS = ExactlyOnce
}

/**
 * The mapping topics to subscribe to and the requested Quality of Service ([[MqttQoS]]).
 */
final class MqttSubscriptions private (
    val subscriptions: Map[String, MqttQoS]
) {
  def withSubscriptions(subscriptions: Map[String, MqttQoS]): MqttSubscriptions =
    new MqttSubscriptions(subscriptions)

  def withSubscriptions(subscriptions: java.util.List[akka.japi.Pair[String, MqttQoS]]): MqttSubscriptions =
    new MqttSubscriptions(subscriptions.asScala.map(_.toScala).toMap)

  def addSubscription(topic: String, qos: MqttQoS): MqttSubscriptions =
    new MqttSubscriptions(this.subscriptions.updated(topic, qos))
}

/**
 * The mapping topics to subscribe to and the requested Quality of Service ([[MqttQoS]]).
 */
object MqttSubscriptions {
  val empty = new MqttSubscriptions(Map.empty)

  /** Scala API */
  def apply(subscriptions: Map[String, MqttQoS]): MqttSubscriptions =
    new MqttSubscriptions(subscriptions)

  /** Scala API */
  def apply(topic: String, qos: MqttQoS): MqttSubscriptions =
    new MqttSubscriptions(Map(topic -> qos))

  /** Scala API */
  def apply(subscription: (String, MqttQoS)): MqttSubscriptions =
    new MqttSubscriptions(Map(subscription))

  /** Java API */
  def create(subscriptions: java.util.List[akka.japi.Pair[String, MqttQoS]]): MqttSubscriptions =
    new MqttSubscriptions(subscriptions.asScala.map(_.toScala).toMap)

  /** Java API */
  def create(topic: String, qos: MqttQoS): MqttSubscriptions =
    new MqttSubscriptions(Map(topic -> qos))

}

/**
 * @param subscriptions the mapping between a topic name and a [[MqttQoS]].
 */
final case class MqttSourceSettings(
    connectionSettings: MqttConnectionSettings,
    subscriptions: Map[String, MqttQoS] = Map.empty
) {
  @annotation.varargs
  def withSubscriptions(subscriptions: akka.japi.Pair[String, MqttQoS]*) =
    copy(subscriptions = subscriptions.map(_.toScala).toMap)
}

object MqttSourceSettings {

  /**
   * Java API: create [[MqttSourceSettings]].
   */
  def create(connectionSettings: MqttConnectionSettings) =
    MqttSourceSettings(connectionSettings)
}

/**
 * Connection settings passed to the underlying Paho client.
 *
 * @see https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttConnectOptions.html
 */
final class MqttConnectionSettings private (val broker: String,
                                            val clientId: String,
                                            val persistence: org.eclipse.paho.client.mqttv3.MqttClientPersistence,
                                            val auth: Option[(String, String)],
                                            val socketFactory: Option[javax.net.ssl.SSLSocketFactory],
                                            val cleanSession: Boolean,
                                            val will: Option[MqttMessage],
                                            val automaticReconnect: Boolean,
                                            val keepAliveInterval: FiniteDuration,
                                            val connectionTimeout: FiniteDuration,
                                            val disconnectQuiesceTimeout: FiniteDuration,
                                            val disconnectTimeout: FiniteDuration,
                                            val maxInFlight: Int,
                                            val mqttVersion: Int,
                                            val serverUris: immutable.Seq[String],
                                            val sslHostnameVerifier: Option[javax.net.ssl.HostnameVerifier],
                                            val sslProperties: Map[String, String]) {

  def withBroker(value: String): MqttConnectionSettings = copy(broker = value)
  def withClientId(clientId: String): MqttConnectionSettings = copy(clientId = clientId)
  def withPersistence(value: org.eclipse.paho.client.mqttv3.MqttClientPersistence): MqttConnectionSettings =
    copy(persistence = value)
  def withAuth(username: String, password: String): MqttConnectionSettings =
    copy(auth = Some((username, password)))
  def withSocketFactory(value: javax.net.ssl.SSLSocketFactory): MqttConnectionSettings =
    copy(socketFactory = Option(value))
  def withCleanSession(value: Boolean): MqttConnectionSettings =
    if (cleanSession == value) this else copy(cleanSession = value)
  def withWill(value: MqttMessage): MqttConnectionSettings = copy(will = Option(value))
  def withAutomaticReconnect(value: Boolean): MqttConnectionSettings =
    if (automaticReconnect == value) this else copy(automaticReconnect = value)

  /** Scala API */
  def withKeepAliveInterval(value: FiniteDuration): MqttConnectionSettings =
    copy(keepAliveInterval = value)

  /** Java API */
  def withKeepAliveInterval(value: java.time.Duration): MqttConnectionSettings =
    withKeepAliveInterval(
      value.asScala
    )

  /** Scala API */
  def withConnectionTimeout(value: FiniteDuration): MqttConnectionSettings =
    copy(connectionTimeout = value)

  /** Java API */
  def withConnectionTimeout(value: java.time.Duration): MqttConnectionSettings =
    withConnectionTimeout(
      value.asScala
    )

  /** Scala API */
  def withDisconnectQuiesceTimeout(value: FiniteDuration): MqttConnectionSettings =
    copy(disconnectQuiesceTimeout = value)

  /** Java API */
  def withDisconnectQuiesceTimeout(value: java.time.Duration): MqttConnectionSettings =
    withDisconnectQuiesceTimeout(
      value.asScala
    )

  /** Scala API */
  def withDisconnectTimeout(value: FiniteDuration): MqttConnectionSettings =
    copy(disconnectTimeout = value)

  /** Java API */
  def withDisconnectTimeout(value: java.time.Duration): MqttConnectionSettings =
    withDisconnectTimeout(
      value.asScala
    )
  def withMaxInFlight(value: Int): MqttConnectionSettings = copy(maxInFlight = value)
  def withMqttVersion(value: Int): MqttConnectionSettings = copy(mqttVersion = value)

  def withServerUri(value: String): MqttConnectionSettings = copy(serverUris = immutable.Seq(value))

  /** Scala API */
  def withServerUris(values: immutable.Seq[String]): MqttConnectionSettings = copy(serverUris = values)

  /** Java API */
  def withServerUris(values: java.util.List[String]): MqttConnectionSettings = copy(serverUris = values.asScala.toList)
  def withSslHostnameVerifier(value: javax.net.ssl.HostnameVerifier): MqttConnectionSettings =
    copy(sslHostnameVerifier = Option(value))

  /** Scala API */
  def withSslProperties(value: Map[String, String]): MqttConnectionSettings = copy(sslProperties = value)

  /** Java API */
  def withSslProperties(value: java.util.Map[String, String]): MqttConnectionSettings =
    copy(sslProperties = value.asScala.toMap)

  /**
   * @deprecated use with [[java.time.Duration]] instead
   */
  @java.lang.Deprecated
  def withKeepAliveInterval(keepAliveInterval: Int, unit: TimeUnit): MqttConnectionSettings =
    copy(keepAliveInterval = FiniteDuration(keepAliveInterval, unit))

  /**
   * @deprecated use with [[java.time.Duration]] instead
   */
  @java.lang.Deprecated
  def withConnectionTimeout(connectionTimeout: Int, unit: TimeUnit): MqttConnectionSettings =
    copy(connectionTimeout = FiniteDuration(connectionTimeout, unit))

  /**
   * @deprecated use with [[java.time.Duration]] instead
   */
  @java.lang.Deprecated
  def withDisconnectQuiesceTimeout(disconnectQuiesceTimeout: Int, unit: TimeUnit): MqttConnectionSettings =
    copy(disconnectQuiesceTimeout = FiniteDuration(disconnectQuiesceTimeout, unit))

  /**
   * @deprecated use with [[java.time.Duration]] instead
   */
  @java.lang.Deprecated
  def withDisconnectTimeout(disconnectTimeout: Int, unit: TimeUnit): MqttConnectionSettings =
    copy(disconnectTimeout = FiniteDuration(disconnectTimeout, unit))

  private def copy(broker: String = broker,
                   clientId: String = clientId,
                   persistence: org.eclipse.paho.client.mqttv3.MqttClientPersistence = persistence,
                   auth: Option[(String, String)] = auth,
                   socketFactory: Option[javax.net.ssl.SSLSocketFactory] = socketFactory,
                   cleanSession: Boolean = cleanSession,
                   will: Option[MqttMessage] = will,
                   automaticReconnect: Boolean = automaticReconnect,
                   keepAliveInterval: FiniteDuration = keepAliveInterval,
                   connectionTimeout: FiniteDuration = connectionTimeout,
                   disconnectQuiesceTimeout: FiniteDuration = disconnectQuiesceTimeout,
                   disconnectTimeout: FiniteDuration = disconnectTimeout,
                   maxInFlight: Int = maxInFlight,
                   mqttVersion: Int = mqttVersion,
                   serverUris: immutable.Seq[String] = serverUris,
                   sslHostnameVerifier: Option[javax.net.ssl.HostnameVerifier] = sslHostnameVerifier,
                   sslProperties: Map[String, java.lang.String] = sslProperties): MqttConnectionSettings =
    new MqttConnectionSettings(
      broker = broker,
      clientId = clientId,
      persistence = persistence,
      auth = auth,
      socketFactory = socketFactory,
      cleanSession = cleanSession,
      will = will,
      automaticReconnect = automaticReconnect,
      keepAliveInterval = keepAliveInterval,
      connectionTimeout = connectionTimeout,
      disconnectQuiesceTimeout = disconnectQuiesceTimeout,
      disconnectTimeout = disconnectTimeout,
      maxInFlight = maxInFlight,
      mqttVersion = mqttVersion,
      serverUris = serverUris,
      sslHostnameVerifier = sslHostnameVerifier,
      sslProperties = sslProperties
    )

  override def toString =
    s"""MqttConnectionSettings(broker=$broker,clientId=$clientId,persistence=$persistence,auth(username)=${auth.map(
      _._1
    )},socketFactory=$socketFactory,cleanSession=$cleanSession,will=$will,automaticReconnect=$automaticReconnect,keepAliveInterval=$keepAliveInterval,connectionTimeout=$connectionTimeout,disconnectQuiesceTimeout=$disconnectQuiesceTimeout,disconnectTimeout=$disconnectTimeout,maxInFlight=$maxInFlight,mqttVersion=$mqttVersion,serverUris=$serverUris,sslHostnameVerifier=$sslHostnameVerifier,sslProperties=$sslProperties)"""
}

/**
 * Factory for connection settings passed to the underlying Paho client.
 *
 * @see https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttConnectOptions.html
 */
object MqttConnectionSettings {

  /** Scala API */
  def apply(
      broker: String,
      clientId: String,
      persistence: MqttClientPersistence
  ): MqttConnectionSettings =
    new MqttConnectionSettings(
      broker,
      clientId,
      persistence,
      auth = None,
      socketFactory = None,
      cleanSession = MqttConnectOptions.CLEAN_SESSION_DEFAULT,
      will = None,
      automaticReconnect = false,
      keepAliveInterval = MqttConnectOptions.KEEP_ALIVE_INTERVAL_DEFAULT.seconds,
      connectionTimeout = MqttConnectOptions.CONNECTION_TIMEOUT_DEFAULT.seconds,
      disconnectQuiesceTimeout = 30.seconds,
      disconnectTimeout = 10.seconds,
      maxInFlight = MqttConnectOptions.MAX_INFLIGHT_DEFAULT,
      mqttVersion = MqttConnectOptions.MQTT_VERSION_3_1_1,
      serverUris = immutable.Seq.empty,
      sslHostnameVerifier = None,
      sslProperties = Map.empty
    )

  /** Java API */
  def create(
      broker: String,
      clientId: String,
      persistence: org.eclipse.paho.client.mqttv3.MqttClientPersistence,
  ): MqttConnectionSettings = apply(
    broker,
    clientId,
    persistence
  )
}
