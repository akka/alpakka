/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.impl

import java.util.Properties
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.mqtt._
import akka.stream.alpakka.mqtt.scaladsl.MqttMessageWithAck
import akka.stream.stage._
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.{
  IMqttActionListener,
  IMqttAsyncClient,
  IMqttDeliveryToken,
  IMqttToken,
  MqttAsyncClient,
  MqttCallbackExtended,
  MqttConnectOptions,
  MqttException,
  MqttMessage => PahoMqttMessage
}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions
import akka.stream.alpakka.mqtt.MqttOfflinePersistenceSettings
import akka.stream.alpakka.mqtt.impl.MqttFlowStageLogic.asActionListener

/**
 * INTERNAL API
 */

@InternalApi
private[mqtt] final class MqttFlowStageWithAck(connectionSettings: MqttConnectionSettings,
                                               subscriptions: Map[String, MqttQoS],
                                               bufferSize: Int,
                                               defaultQoS: MqttQoS,
                                               manualAcks: Boolean = false)
    extends GraphStageWithMaterializedValue[FlowShape[MqttMessageWithAck, MqttMessageWithAck], Future[Done]] {

  private val in = Inlet[MqttMessageWithAck]("MqttFlow.in")
  private val out = Outlet[MqttMessageWithAck]("MqttFlow.out")
  override val shape: Shape = FlowShape(in, out)

  override protected def initialAttributes: Attributes = Attributes.name("MqttFlow")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val subscriptionPromise = Promise[Done]

    new MqttFlowStageLogic[MqttMessageWithAck](in,
                                               out,
                                               shape,
                                               subscriptionPromise,
                                               connectionSettings,
                                               subscriptions,
                                               bufferSize,
                                               defaultQoS,
                                               manualAcks)
    val logic = new MqttFlowWithAckStageLogic(in,
                                              out,
                                              shape,
                                              subscriptionPromise,
                                              connectionSettings,
                                              subscriptions,
                                              bufferSize,
                                              defaultQoS,
                                              manualAcks)
    (logic, subscriptionPromise.future)
  }

}

class MqttFlowWithAckStageLogic(in: Inlet[MqttMessageWithAck],
                                out: Outlet[MqttMessageWithAck],
                                shape: Shape,
                                subscriptionPromise: Promise[Done],
                                connectionSettings: MqttConnectionSettings,
                                subscriptions: Map[String, MqttQoS],
                                bufferSize: Int,
                                defaultQoS: MqttQoS,
                                manualAcks: Boolean)
    extends MqttFlowStageLogic[MqttMessageWithAck](in,
                                                   out,
                                                   shape,
                                                   subscriptionPromise,
                                                   connectionSettings,
                                                   subscriptions,
                                                   bufferSize,
                                                   defaultQoS,
                                                   manualAcks) {

  private val messagesToAck: mutable.HashMap[Int, MqttMessageWithAck] = mutable.HashMap()

  override def handleDeliveryComplete(token: IMqttDeliveryToken): Unit =
    if (messagesToAck.isDefinedAt(token.getMessageId)) {
      messagesToAck(token.getMessageId).ack()
      messagesToAck.remove(token.getMessageId)
    }

  override def publishToMqttWithAck(msg: MqttMessageWithAck): IMqttDeliveryToken = {
    val publish = publishToMqtt(msg.message)
    messagesToAck ++= mutable.HashMap(publish.getMessageId -> msg)
    publish
  }

}
