/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import javax.net.ssl.{HostnameVerifier, SSLSocketFactory}
import org.eclipse.paho.client.mqttv3.{MqttClientPersistence, MqttConnectOptions}

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, Seq}
import scala.concurrent.duration._

sealed abstract class MqttQoS {
  def byteValue: Byte
}

/**
 * Quality of Service constants as defined in
 * See http://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttMessage.html#setQos-int-
 */
object MqttQoS {
  object AtMostOnce extends MqttQoS {
    val byteValue: Byte = 0
  }

  object AtLeastOnce extends MqttQoS {
    val byteValue: Byte = 1
  }

  object ExactlyOnce extends MqttQoS {
    val byteValue: Byte = 2
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
 * @see https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttConnectOptions.html
 */
final case class MqttConnectionSettings(
    broker: String,
    clientId: String,
    persistence: MqttClientPersistence,
    auth: Option[(String, String)] = None,
    socketFactory: Option[SSLSocketFactory] = None,
    cleanSession: Boolean = true,
    will: Option[MqttMessage] = None,
    automaticReconnect: Boolean = false,
    keepAliveInterval: FiniteDuration = 60.seconds,
    connectionTimeout: FiniteDuration = 30.seconds,
    disconnectQuiesceTimeout: FiniteDuration = 30.seconds,
    disconnectTimeout: FiniteDuration = 10.seconds,
    maxInFlight: Int = 10,
    mqttVersion: Int = MqttConnectOptions.MQTT_VERSION_3_1_1,
    serverUris: Seq[String] = Seq.empty,
    sslHostnameVerifier: Option[HostnameVerifier] = None,
    sslProperties: Map[String, String] = Map.empty
) {
  def withBroker(broker: String): MqttConnectionSettings =
    copy(broker = broker)

  def withAuth(username: String, password: String): MqttConnectionSettings =
    copy(auth = Some((username, password)))

  def withCleanSession(cleanSession: Boolean): MqttConnectionSettings =
    copy(cleanSession = cleanSession)

  def withWill(will: MqttMessage): MqttConnectionSettings =
    copy(will = Some(will))

  def withClientId(clientId: String): MqttConnectionSettings =
    copy(clientId = clientId)

  def withAutomaticReconnect(automaticReconnect: Boolean): MqttConnectionSettings =
    copy(automaticReconnect = automaticReconnect)

  def withKeepAliveInterval(keepAliveInterval: Int, unit: TimeUnit): MqttConnectionSettings =
    copy(keepAliveInterval = FiniteDuration(keepAliveInterval, unit))

  def withConnectionTimeout(connectionTimeout: Int, unit: TimeUnit): MqttConnectionSettings =
    copy(connectionTimeout = FiniteDuration(connectionTimeout, unit))

  def withDisconnectQuiesceTimeout(disconnectQuiesceTimeout: Int, unit: TimeUnit): MqttConnectionSettings =
    copy(disconnectQuiesceTimeout = FiniteDuration(disconnectQuiesceTimeout, unit))

  def withDisconnectTimeout(disconnectTimeout: Int, unit: TimeUnit): MqttConnectionSettings =
    copy(disconnectTimeout = FiniteDuration(disconnectTimeout, unit))

  def withMaxInFlight(maxInFlight: Int): MqttConnectionSettings =
    copy(maxInFlight = maxInFlight)

  def withMqttVersion(mqttVersion: Int): MqttConnectionSettings =
    copy(mqttVersion = mqttVersion)

  @varargs def withServerUris(serverUris: String*): MqttConnectionSettings =
    copy(serverUris = serverUris.to[Seq])

  def withSslHostnameVerifier(sslHostnameVerifier: HostnameVerifier): MqttConnectionSettings =
    copy(sslHostnameVerifier = Some(sslHostnameVerifier))

  def withSslProperties(sslProperties: java.util.Map[String, String]): MqttConnectionSettings =
    copy(sslProperties = sslProperties.asScala.toMap)
}

object MqttConnectionSettings {

  /**
   * Java API: create [[MqttConnectionSettings]] with no auth information.
   */
  def create(broker: String, clientId: String, persistence: MqttClientPersistence) =
    MqttConnectionSettings(broker, clientId, persistence)
}
