/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import javax.net.ssl.SSLSocketFactory

import akka.Done
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.{IMqttActionListener, IMqttToken, MqttClientPersistence}

import scala.concurrent.Promise
import scala.language.implicitConversions
import scala.util._

sealed abstract class MqttQoS {
  def byteValue: Byte
}

/**
 * Quality of Service constants as defined in
 * https://www.eclipse.org/paho/files/mqttdoc/Cclient/qos.html
 */
object MqttQoS {
  object AtMostOnce extends MqttQoS {
    def byteValue: Byte = 0
  }

  object AtLeastOnce extends MqttQoS {
    def byteValue: Byte = 1
  }

  object ExactlyOnce extends MqttQoS {
    def byteValue: Byte = 2
  }

  /**
   * Java API
   */
  def atMostOnce = AtMostOnce

  /**
   * Java API
   */
  def atLeastOnce = AtLeastOnce

  /**
   * Java API
   */
  def exactlyOnce = ExactlyOnce
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

final case class MqttConnectionSettings(
    broker: String,
    clientId: String,
    persistence: MqttClientPersistence,
    auth: Option[(String, String)] = None,
    socketFactory: Option[SSLSocketFactory] = None,
    cleanSession: Boolean = true,
    will: Option[MqttMessage] = None
) {
  def withBroker(broker: String) =
    copy(broker = broker)

  def withAuth(username: String, password: String) =
    copy(auth = Some((username, password)))

  def withCleanSession(cleanSession: Boolean) =
    copy(cleanSession = cleanSession)

  def withWill(will: MqttMessage) =
    copy(will = Some(will))

  @deprecated("use a normal message instead of a will", "0.16")
  def withWill(will: Will) =
    copy(will = Some(MqttMessage(will.message.topic, will.message.payload, Some(will.qos), will.retained)))

  def withClientId(clientId: String) =
    copy(clientId = clientId)
}

object MqttConnectionSettings {

  /**
   * Java API: create [[MqttConnectionSettings]] with no auth information.
   */
  def create(broker: String, clientId: String, persistence: MqttClientPersistence) =
    MqttConnectionSettings(broker, clientId, persistence)
}

final case class MqttMessage(topic: String,
                             payload: ByteString,
                             qos: Option[MqttQoS] = None,
                             retained: Boolean = false)

@deprecated("use a normal message instead of a will", "0.16")
final case class Will(message: MqttMessage, qos: MqttQoS, retained: Boolean)

final case class CommitCallbackArguments(messageId: Int, qos: MqttQoS, promise: Promise[Done])

object MqttMessage {

  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString) =
    MqttMessage(topic, payload)

  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString, qos: MqttQoS) =
    MqttMessage(topic, payload, Some(qos))

  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString, retained: Boolean) =
    MqttMessage(topic, payload, retained = retained)

  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString, qos: MqttQoS, retained: Boolean) =
    MqttMessage(topic, payload, Some(qos), retained = retained)
}

/**
 *  Internal API
 */
private[mqtt] object MqttConnectorLogic {

  implicit def funcToMqttActionListener(func: Try[IMqttToken] => Unit): IMqttActionListener = new IMqttActionListener {
    def onSuccess(token: IMqttToken) = func(Success(token))
    def onFailure(token: IMqttToken, ex: Throwable) = func(Failure(ex))
  }

}
