/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package scaladsl

import akka.NotUsed
import akka.stream.alpakka.mqtt.streaming.impl.MqttFrameStage
import akka.stream.scaladsl.{BidiFlow, Flow}
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
  ): BidiFlow[Command[_], ByteString, ByteString, Either[MqttCodec.DecodeError, Event[_]], NotUsed] = {
    // TODO: Have the input and output flows pass through an actor that maintains session state (probably an FSM)
    import MqttCodec._
    BidiFlow
      .fromFlows(
        Flow[Command[_]].map {
          case Command(cp: Connect, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: Publish, _) => cp.encode(ByteString.newBuilder, cp.packetId).result()
          case Command(cp: PubRec, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: PubRel, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: PubComp, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: Subscribe, _) => cp.encode(ByteString.newBuilder, cp.packetId).result()
          case Command(cp: Unsubscribe, _) => cp.encode(ByteString.newBuilder, cp.packetId).result()
          case Command(cp: PingReq.type, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: Disconnect.type, _) => cp.encode(ByteString.newBuilder).result()
          case c: Command[_] => throw new IllegalStateException(c + " is not a client command")
        },
        Flow[ByteString]
          .via(new MqttFrameStage(settings.maxPacketSize))
          .map(_.iterator.decodeControlPacket(settings.maxPacketSize).map(Event(_, None)))
      )
  }

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
  ): BidiFlow[Command[_], ByteString, ByteString, Either[MqttCodec.DecodeError, Event[_]], NotUsed] = {
    // TODO: Have the input and output flows pass through an actor that maintains session state (probably an FSM)
    import MqttCodec._
    BidiFlow
      .fromFlows(
        Flow[Command[_]].map {
          case Command(cp: ConnAck, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: Publish, _) => cp.encode(ByteString.newBuilder, cp.packetId).result()
          case Command(cp: PubAck, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: PubRec, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: PubRel, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: PubComp, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: SubAck, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: UnsubAck, _) => cp.encode(ByteString.newBuilder).result()
          case Command(cp: PingResp.type, _) => cp.encode(ByteString.newBuilder).result()
          case c: Command[_] => throw new IllegalStateException(c + " is not a server command")
        },
        Flow[ByteString]
          .via(new MqttFrameStage(settings.maxPacketSize))
          .map(_.iterator.decodeControlPacket(settings.maxPacketSize).map(Event(_, None)))
      )
  }
}
