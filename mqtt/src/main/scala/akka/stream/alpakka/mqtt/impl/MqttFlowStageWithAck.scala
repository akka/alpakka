/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.mqtt.{MqttConnectionSettings, MqttQoS}
import akka.stream.alpakka.mqtt.scaladsl.MqttMessageWithAck
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}

import scala.concurrent.Future

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

  private val mqttFlowStage = new MqttFlowStage(connectionSettings, subscriptions, bufferSize, defaultQoS, manualAcks)
  private val in = Inlet[MqttMessageWithAck]("MqttFlow.in")
  private val out = Outlet[MqttMessageWithAck]("MqttFlow.out")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) =
    mqttFlowStage.createLogicAndMaterializedValue(inheritedAttributes)

  override val shape: Shape = FlowShape(in, out)
}
