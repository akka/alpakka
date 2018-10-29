/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package scaladsl

import akka.NotUsed
import akka.stream.scaladsl.BidiFlow
import akka.util.ByteString

object Mqtt {

  /**
   * Create a bidirectional flow that maintains client session state with an MQTT endpoint.
   * The bidirectional flow can be joined with an endpoint flow that receives
   * [[ByteString]] payloads and independently produces [[ByteString]] payloads e.g.
   * an MQTT server.
   *
   * @param session the MQTT client session to use
   * @return the bidirectional flow
   */
  def clientSessionFlow[A](
      session: MqttClientSession
  ): BidiFlow[Command[A], ByteString, ByteString, Either[MqttCodec.DecodeError, Event[A]], NotUsed] =
    BidiFlow.fromFlows(session.commandFlow, session.eventFlow)

  /**
   * Create a bidirectional flow that maintains server session state with an MQTT endpoint.
   * The bidirectional flow can be joined with an endpoint flow that receives
   * [[ByteString]] payloads and independently produces [[ByteString]] payloads e.g.
   * an MQTT server.
   *
   * @param session the MQTT server session to use
   * @param connectionId a identifier to distinguish the client connection so that the session
   *                     can route the incoming requests
   * @return the bidirectional flow
   */
  def serverSessionFlow[A](
      session: MqttServerSession,
      connectionId: ByteString
  ): BidiFlow[Command[A], ByteString, ByteString, Either[MqttCodec.DecodeError, Event[A]], NotUsed] =
    BidiFlow.fromFlows(session.commandFlow(connectionId), session.eventFlow(connectionId))
}
