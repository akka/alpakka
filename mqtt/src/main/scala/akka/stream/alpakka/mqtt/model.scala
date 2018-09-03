/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt

import akka.Done
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.{IMqttActionListener, IMqttToken}

import scala.concurrent.Promise
import scala.language.implicitConversions
import scala.util._

final case class MqttMessage(topic: String, payload: ByteString, qos: Option[MqttQoS] = None, retained: Boolean = false)

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
