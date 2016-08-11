/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.mqtt

import akka.util.ByteString
import akka.stream.stage._
import org.eclipse.paho.client.mqttv3.{ MqttMessage => PahoMqttMessage, _ }
import scala.util._
import scala.language.implicitConversions

case class MqttSourceSettings(
  connectionSettings: MqttConnectionSettings,
  topics:             Map[String, Int]
)

case class MqttConnectionSettings(
  broker:      String,
  clientId:    String,
  persistence: MqttClientPersistence
)

case class MqttMessage(topic: String, payload: ByteString)

trait MqttConnectorLogic { this: GraphStageLogic =>

  import MqttConnectorLogic._

  def connectionSettings: MqttConnectionSettings
  def onConnect: AsyncCallback[IMqttAsyncClient]
  def onMessage: AsyncCallback[MqttMessage]
  def onConnectionLost: AsyncCallback[Throwable]

  final override def preStart(): Unit = {
    val client = new MqttAsyncClient(
      connectionSettings.broker,
      connectionSettings.clientId,
      connectionSettings.persistence
    )

    client.connect((), connectHandler(client))
    client.setCallback(new MqttCallback {
      def messageArrived(topic: String, message: PahoMqttMessage) =
        onMessage.invoke(MqttMessage(topic, ByteString(message.getPayload)))

      def deliveryComplete(token: IMqttDeliveryToken) =
        println(s"Delivery complete $token")

      def connectionLost(cause: Throwable) =
        onConnectionLost.invoke(cause)
    })
  }

  private val connectHandler: IMqttAsyncClient => Try[IMqttToken] => Unit = client => {
    case Success(_)  => onConnect.invoke(client)
    case Failure(ex) => onConnectionLost.invoke(ex)
  }
}

object MqttConnectorLogic {

  implicit def funcToMqttActionListener(func: Try[IMqttToken] => Unit): IMqttActionListener = new IMqttActionListener {
    def onSuccess(token: IMqttToken) = func(Success(token))
    def onFailure(token: IMqttToken, ex: Throwable) = func(Failure(ex))
  }

}
