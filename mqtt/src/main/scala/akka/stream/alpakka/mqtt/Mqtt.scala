/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.mqtt

import akka.stream.stage._
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.{ MqttMessage => PahoMqttMessage, _ }

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
  subscriptions:      Map[String, MqttQoS]   = Map.empty
) {
  @annotation.varargs
  def withSubscriptions(subscription: akka.japi.Pair[String, MqttQoS], subscriptions: akka.japi.Pair[String, MqttQoS]*) =
    copy(subscriptions = (subscription +: subscriptions).map(_.toScala).toMap)
}

object MqttSourceSettings {
  /**
   * Java API: create [[MqttSourceSettings]].
   */
  def create(connectionSettings: MqttConnectionSettings) =
    MqttSourceSettings(connectionSettings)
}

final case class MqttConnectionSettings(
  broker:      String,
  clientId:    String,
  persistence: MqttClientPersistence,
  auth:        Option[(String, String)] = None
) {
  def withAuth(username: String, password: String) =
    copy(auth = Some((username, password)))
}

object MqttConnectionSettings {
  /**
   * Java API: create [[MqttConnectionSettings]] with no auth information.
   */
  def create(broker: String, clientId: String, persistence: MqttClientPersistence) =
    MqttConnectionSettings(broker, clientId, persistence)
}

final case class MqttMessage(topic: String, payload: ByteString)

object MqttMessage {
  /**
   * Java API: create  [[MqttMessage]]
   */
  def create(topic: String, payload: ByteString) =
    MqttMessage(topic, payload)
}

/**
 *  Internal API
 */
private[mqtt] trait MqttConnectorLogic { this: GraphStageLogic =>

  import MqttConnectorLogic._

  def connectionSettings: MqttConnectionSettings
  def handleConnection(client: IMqttAsyncClient): Unit

  /**
   * Callback, that is called from the MQTT client thread before invoking
   * message handler callback in the GraphStage context.
   */
  def beforeHandleMessage(): Unit

  def handleMessage(message: MqttMessage): Unit
  def handleConnectionLost(ex: Throwable): Unit

  val onConnect = getAsyncCallback[IMqttAsyncClient](handleConnection)
  val onMessage = getAsyncCallback[MqttMessage](handleMessage)
  val onConnectionLost = getAsyncCallback[Throwable](handleConnectionLost)

  final override def preStart(): Unit = {
    val client = new MqttAsyncClient(
      connectionSettings.broker,
      connectionSettings.clientId,
      connectionSettings.persistence
    )

    client.setCallback(new MqttCallback {
      def messageArrived(topic: String, message: PahoMqttMessage) = {
        beforeHandleMessage()
        onMessage.invoke(MqttMessage(topic, ByteString(message.getPayload)))
      }

      def deliveryComplete(token: IMqttDeliveryToken) =
        println(s"Delivery complete $token")

      def connectionLost(cause: Throwable) =
        onConnectionLost.invoke(cause)
    })
    val connectOptions = new MqttConnectOptions
    connectionSettings.auth.foreach {
      case (user, password) =>
        connectOptions.setUserName(user)
        connectOptions.setPassword(password.toCharArray)
    }
    client.connect(connectOptions, (), connectHandler(client))
  }

  private val connectHandler: IMqttAsyncClient => Try[IMqttToken] => Unit = client => {
    case Success(_)  => onConnect.invoke(client)
    case Failure(ex) => onConnectionLost.invoke(ex)
  }
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
