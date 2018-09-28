/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package javadsl

import akka.NotUsed
import akka.stream.alpakka.mqtt.streaming.MqttCodec.DecodeError
import akka.stream.javadsl.BidiFlow
import akka.stream.scaladsl.{BidiFlow => ScalaBidiFlow}
import akka.util.ByteString

object Mqtt {

  /**
   * Create a bidirectional flow that maintains client session state with an MQTT endpoint.
   * The bidirectional flow can be joined with an endpoint flow that receives
   * [[ByteString]] payloads and independently produces [[ByteString]] payloads e.g.
   * an MQTT server.
   *
   * @param settings settings for the session
   * @return the bidirectional flow
   */
  def clientSessionFlow(
      settings: SessionFlowSettings
  ): BidiFlow[Command[_], ByteString, ByteString, DecodeErrorOrEvent, NotUsed] =
    inputOutputConverter
      .atop(scaladsl.Mqtt.clientSessionFlow(settings))
      .asJava

  /**
   * Create a bidirectional flow that maintains server session state with an MQTT endpoint.
   * The bidirectional flow can be joined with an endpoint flow that receives
   * [[ByteString]] payloads and independently produces [[ByteString]] payloads e.g.
   * an MQTT server.
   *
   * @param settings settings for the session
   * @return the bidirectional flow
   */
  def serverSessionFlow(
      settings: SessionFlowSettings
  ): BidiFlow[Command[_], ByteString, ByteString, DecodeErrorOrEvent, NotUsed] =
    inputOutputConverter
      .atop(scaladsl.Mqtt.serverSessionFlow(settings))
      .asJava

  /*
   * Converts Java inputs to Scala, and vice-versa.
   */
  private val inputOutputConverter =
    ScalaBidiFlow
      .fromFunctions[Command[_], Command[_], Either[DecodeError, Event[_]], DecodeErrorOrEvent](
        identity,
        DecodeErrorOrEvent.apply
      )
}
