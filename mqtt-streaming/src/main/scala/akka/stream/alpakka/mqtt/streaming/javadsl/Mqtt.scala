/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package javadsl
import akka.NotUsed
import akka.stream.alpakka.mqtt.streaming.MqttCodec.{ControlPacketResult, DecodeError}
import akka.stream.javadsl.BidiFlow
import akka.stream.scaladsl.{BidiFlow => ScalaBidiFlow}
import akka.util.ByteString

object Mqtt {

  /**
   * Create a bidirectional flow that maintains session state with an MQTT endpoint.
   * The bidirectional flow can be joined with an endpoint flow that receives
   * [[ByteString]] payloads and independently produces [[ByteString]] payloads e.g.
   * an MQTT server.
   *
   * @param settings settings for the session
   * @return the bidirectional flow
   */
  def sessionFlow(
      settings: SessionFlowSettings
  ): BidiFlow[ControlPacket, ByteString, ByteString, ControlPacketResult, NotUsed] =
    ScalaBidiFlow
      .fromFunctions[ControlPacket, ControlPacket, Either[DecodeError, ControlPacket], ControlPacketResult](
        identity,
        ControlPacketResult.apply
      )
      .atop(scaladsl.Mqtt.sessionFlow(settings))
      .asJava
}
