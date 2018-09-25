/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package scaladsl

import akka.NotUsed
import akka.stream.alpakka.mqtt.streaming.MqttCodec.DecodeError
import akka.stream.alpakka.mqtt.streaming.impl.MqttFrameStage
import akka.stream.scaladsl.{BidiFlow, Flow}
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
  ): BidiFlow[ControlPacket, ByteString, ByteString, Either[DecodeError, ControlPacket], NotUsed] = {
    // TODO: Have the input and output flows pass through an actor that maintains session state (probably an FSM)
    import MqttCodec._
    BidiFlow
      .fromFlows(
        Flow[ControlPacket].map {
          case cp: Connect => cp.encode(ByteString.newBuilder).result()
          case cp: ConnAck => cp.encode(ByteString.newBuilder).result()
          case cp: Publish => cp.encode(ByteString.newBuilder).result()
          case cp: PubAck => cp.encode(ByteString.newBuilder).result()
          case cp: PubRec => cp.encode(ByteString.newBuilder).result()
          case cp: PubRel => cp.encode(ByteString.newBuilder).result()
          case cp: PubComp => cp.encode(ByteString.newBuilder).result()
          case cp: Subscribe => cp.encode(ByteString.newBuilder).result()
          case cp: SubAck => cp.encode(ByteString.newBuilder).result()
          case cp: Unsubscribe => cp.encode(ByteString.newBuilder).result()
          case cp: UnsubAck => cp.encode(ByteString.newBuilder).result()
          case cp: PingReq.type => cp.encode(ByteString.newBuilder).result()
          case cp: PingResp.type => cp.encode(ByteString.newBuilder).result()
          case cp: Disconnect.type => cp.encode(ByteString.newBuilder).result()
          case cp: Reserved1.type => cp.encode(ByteString.newBuilder, 0).result()
          case cp: Reserved2.type => cp.encode(ByteString.newBuilder, 0).result()
        },
        Flow[ByteString]
          .via(new MqttFrameStage(settings.maxPacketSize))
          .map(_.iterator.decodeControlPacket(settings.maxPacketSize))
      )
  }
}
