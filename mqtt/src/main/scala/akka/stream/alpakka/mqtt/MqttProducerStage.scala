/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.mqtt

import akka.stream._
import akka.stream.stage._
import org.eclipse.paho.client.mqttv3.{IMqttAsyncClient, IMqttToken, MqttMessage => PahoMqttMessage}

import scala.util.{Failure, Success, Try}

final class MqttProducerStage(cs: MqttConnectionSettings, qos: MqttQoS)
    extends GraphStage[FlowShape[MqttMessage, MqttMessage]] {

  import MqttConnectorLogic._

  val in = Inlet[MqttMessage]("MqttProducer.in")
  val out = Outlet[MqttMessage]("MqttProducer.out")

  override val shape = FlowShape.of(in, out)
  override protected def initialAttributes: Attributes = Attributes.name("MqttProducer")

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) with MqttConnectorLogic {

    private var mqttClient: Option[IMqttAsyncClient] = None
    private val onPublished = getAsyncCallback[Try[IMqttToken]] {
      case Success(token) =>
        push(out, token.getUserContext.asInstanceOf[MqttMessage])
      case Failure(ex) =>
        failStage(ex)
    }

    setHandler(
      in,
      new InHandler {
        override def onPush() = {
          val msg = grab(in)
          val pahoMsg = new PahoMqttMessage(msg.payload.toArray)
          pahoMsg.setQos(qos.byteValue)
          mqttClient match {
            case Some(client) => client.publish(msg.topic, pahoMsg, msg, onPublished.invoke _)
            case None => failStage(MqttProducerStage.NoClientException)
          }

        }
      }
    )

    setHandler(out, new OutHandler {
      override def onPull() =
        if (mqttClient.isDefined) pull(in)
    })

    override def connectionSettings = cs

    override def handleConnection(client: IMqttAsyncClient): Unit = {
      mqttClient = Some(client)
      if (isAvailable(out)) pull(in) // if pull from downstream arrived before we got a client
    }

    override def handleConnectionLost(ex: Throwable): Unit =
      failStage(ex)

    override def postStop() =
      mqttClient.foreach {
        case c if c.isConnected => c.disconnect
        case _ =>
      }
  }
}

object MqttProducerStage {
  final object NoClientException extends Exception("No MQTT client.")
}
