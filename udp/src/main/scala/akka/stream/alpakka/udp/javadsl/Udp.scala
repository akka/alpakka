/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.udp.javadsl

import java.net.InetSocketAddress
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.udp.UdpMessage
import akka.stream.javadsl.{Flow, Sink}
import akka.stream.alpakka.udp.scaladsl

import scala.compat.java8.FutureConverters._

object Udp {

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   */
  def sendFlow(sys: ActorSystem): Flow[UdpMessage, UdpMessage, NotUsed] =
    scaladsl.Udp.sendFlow()(sys).asJava

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   */
  def sendSink(sys: ActorSystem): Sink[UdpMessage, NotUsed] =
    scaladsl.Udp.sendSink()(sys).asJava

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   */
  def bindFlow(localAddress: InetSocketAddress,
               sys: ActorSystem): Flow[UdpMessage, UdpMessage, CompletionStage[InetSocketAddress]] =
    scaladsl.Udp.bindFlow(localAddress)(sys).mapMaterializedValue(_.toJava).asJava
}
