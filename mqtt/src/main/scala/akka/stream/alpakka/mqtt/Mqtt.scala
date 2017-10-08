/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import javax.net.ssl.SSLSocketFactory

import akka.Done
import akka.stream.alpakka.mqtt.scaladsl.MqttCommittableMessage
import akka.stream.stage._
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.{MqttMessage => PahoMqttMessage, _}

import scala.concurrent.{Future, Promise}
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
  def withSubscriptions(subscription: akka.japi.Pair[String, MqttQoS],
                        subscriptions: akka.japi.Pair[String, MqttQoS]*) =
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
    broker: String,
    clientId: String,
    persistence: MqttClientPersistence,
    auth: Option[(String, String)] = None,
    socketFactory: Option[SSLSocketFactory] = None,
    cleanSession: Boolean = true,
    will: Option[Will] = None
) {
  def withBroker(broker: String) =
    copy(broker = broker)

  def withAuth(username: String, password: String) =
    copy(auth = Some((username, password)))

  def withCleanSession(cleanSession: Boolean) =
    copy(cleanSession = cleanSession)

  def withWill(will: Will) =
    copy(will = Some(will))

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

final case class MqttMessage(topic: String, payload: ByteString)

final case class Will(message: MqttMessage, qos: MqttQoS, retained: Boolean)

final case class CommitCallbackArguments(messageId: Int, qos: MqttQoS, promise: Promise[Done])

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
  def handleConnectionLost(ex: Throwable): Unit
  def commitCallback(args: CommitCallbackArguments): Unit = ()

  val onConnect = getAsyncCallback[IMqttAsyncClient](handleConnection)
  val onConnectionLost = getAsyncCallback[Throwable](handleConnectionLost)
  val commitAsyncCallback = getAsyncCallback[CommitCallbackArguments](commitCallback)

  /**
   * Callback, that is called from the MQTT client thread before invoking
   * message handler callback in the GraphStage context.
   */
  def onMessage(message: MqttCommittableMessage) = ()

  final override def preStart(): Unit = {
    val client = new MqttAsyncClient(
      connectionSettings.broker,
      connectionSettings.clientId,
      connectionSettings.persistence
    )

    client.setCallback(new MqttCallback {
      def messageArrived(topic: String, pahoMessage: PahoMqttMessage) =
        onMessage(new MqttCommittableMessage {
          override val message = MqttMessage(topic, ByteString(pahoMessage.getPayload))
          override def messageArrivedComplete(): Future[Done] = {
            val promise = Promise[Done]()
            val qos = pahoMessage.getQos match {
              case 0 => MqttQoS.atMostOnce
              case 1 => MqttQoS.atLeastOnce
              case 2 => MqttQoS.exactlyOnce
            }
            commitAsyncCallback.invoke(CommitCallbackArguments(pahoMessage.getId, qos, promise))
            promise.future
          }
        })

      def deliveryComplete(token: IMqttDeliveryToken) =
        ()

      def connectionLost(cause: Throwable) =
        onConnectionLost.invoke(cause)
    })
    val connectOptions = new MqttConnectOptions
    connectionSettings.auth.foreach {
      case (user, password) =>
        connectOptions.setUserName(user)
        connectOptions.setPassword(password.toCharArray)
    }
    connectionSettings.socketFactory.foreach { socketFactory =>
      connectOptions.setSocketFactory(socketFactory)
    }
    connectionSettings.will.foreach { will =>
      connectOptions.setWill(will.message.topic, will.message.payload.toArray, will.qos.byteValue.toInt, will.retained)
    }
    connectOptions.setCleanSession(connectionSettings.cleanSession)
    client.connect(connectOptions, (), connectHandler)
  }

  private val connectHandler: Try[IMqttToken] => Unit = {
    case Success(token) => onConnect.invoke(token.getClient)
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
