/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.mqtt

import akka.util.ByteString
import akka.stream.stage._
import org.eclipse.paho.client.mqttv3.{ MqttMessage => PahoMqttMessage, _ }
import scala.util._
import scala.language.implicitConversions

final case class MqttSourceSettings(
  connectionSettings: MqttConnectionSettings,
  topics:             Map[String, Int]
)

final case class MqttConnectionSettings(
  broker:      String,
  clientId:    String,
  persistence: MqttClientPersistence
)

final case class MqttMessage(topic: String, payload: ByteString)

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
    client.connect((), connectHandler(client))
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
