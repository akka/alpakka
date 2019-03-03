/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.udp.javadsl

import java.net.InetSocketAddress
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.udp.Datagram
import akka.stream.javadsl.{Flow, Sink}
import akka.stream.alpakka.udp.scaladsl

import scala.compat.java8.FutureConverters._

object Udp {

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   */
  def sendFlow(system: ActorSystem): Flow[Datagram, Datagram, NotUsed] =
    scaladsl.Udp.sendFlow()(system).asJava

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   */
  def sendSink(system: ActorSystem): Sink[Datagram, NotUsed] =
    scaladsl.Udp.sendSink()(system).asJava

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   */
  def bindFlow(localAddress: InetSocketAddress,
               system: ActorSystem): Flow[Datagram, Datagram, CompletionStage[InetSocketAddress]] =
    scaladsl.Udp.bindFlow(localAddress)(system).mapMaterializedValue(_.toJava).asJava
}
