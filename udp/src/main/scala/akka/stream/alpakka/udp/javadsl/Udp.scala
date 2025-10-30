/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.udp.javadsl

import java.net.InetSocketAddress
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.io.Inet.SocketOption
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.stream.alpakka.udp.Datagram
import akka.stream.javadsl.{Flow, Sink}
import akka.stream.alpakka.udp.scaladsl

import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

object Udp {
  import java.lang.{Iterable => JIterable}

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   *
   * @param system the actor system
   */
  def sendFlow(system: ActorSystem): Flow[Datagram, Datagram, NotUsed] =
    scaladsl.Udp.sendFlow()(system).asJava

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   *
   * @param system the actor system
   */
  def sendFlow(system: ClassicActorSystemProvider): Flow[Datagram, Datagram, NotUsed] =
    scaladsl.Udp.sendFlow()(system).asJava

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   *
   * @param options UDP socket options
   * @param system the actor system
   */
  def sendFlow(options: JIterable[SocketOption], system: ActorSystem): Flow[Datagram, Datagram, NotUsed] =
    scaladsl.Udp.sendFlow(options.asScala.toIndexedSeq)(system).asJava

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   *
   * @param options UDP socket options
   * @param system the actor system
   */
  def sendFlow(options: JIterable[SocketOption],
               system: ClassicActorSystemProvider): Flow[Datagram, Datagram, NotUsed] =
    scaladsl.Udp.sendFlow(options.asScala.toIndexedSeq)(system).asJava

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   *
   * @param system the actor system
   */
  def sendSink(system: ActorSystem): Sink[Datagram, NotUsed] =
    scaladsl.Udp.sendSink()(system).asJava

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   *
   * @param system the actor system
   */
  def sendSink(system: ClassicActorSystemProvider): Sink[Datagram, NotUsed] =
    scaladsl.Udp.sendSink()(system).asJava

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   *
   * @param options UDP socket options
   * @param system the actor system
   */
  def sendSink(options: JIterable[SocketOption], system: ActorSystem): Sink[Datagram, NotUsed] =
    scaladsl.Udp.sendSink(options.asScala.toIndexedSeq)(system).asJava

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   *
   * @param options UDP socket options
   * @param system the actor system
   */
  def sendSink(options: JIterable[SocketOption], system: ClassicActorSystemProvider): Sink[Datagram, NotUsed] =
    scaladsl.Udp.sendSink(options.asScala.toIndexedSeq)(system).asJava

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   *
   * @param localAddress UDP socket address
   * @param system the actor system
   */
  def bindFlow(localAddress: InetSocketAddress,
               system: ActorSystem): Flow[Datagram, Datagram, CompletionStage[InetSocketAddress]] =
    scaladsl.Udp.bindFlow(localAddress)(system).mapMaterializedValue(_.asJava).asJava

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   *
   * @param localAddress UDP socket address
   * @param system the actor system
   */
  def bindFlow(localAddress: InetSocketAddress,
               system: ClassicActorSystemProvider): Flow[Datagram, Datagram, CompletionStage[InetSocketAddress]] =
    scaladsl.Udp.bindFlow(localAddress)(system).mapMaterializedValue(_.asJava).asJava

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   *
   * @param localAddress UDP socket address
   * @param options UDP socket options
   * @param system the actor system
   */
  def bindFlow(localAddress: InetSocketAddress,
               options: JIterable[SocketOption],
               system: ActorSystem): Flow[Datagram, Datagram, CompletionStage[InetSocketAddress]] =
    scaladsl.Udp.bindFlow(localAddress, options.asScala.toIndexedSeq)(system).mapMaterializedValue(_.asJava).asJava

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   *
   * @param localAddress UDP socket address
   * @param options UDP socket options
   * @param system the actor system
   */
  def bindFlow(localAddress: InetSocketAddress,
               options: JIterable[SocketOption],
               system: ClassicActorSystemProvider): Flow[Datagram, Datagram, CompletionStage[InetSocketAddress]] =
    scaladsl.Udp.bindFlow(localAddress, options.asScala.toIndexedSeq)(system).mapMaterializedValue(_.asJava).asJava
}
