/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.udp.scaladsl

import java.net.InetSocketAddress

import akka.NotUsed
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.io.Inet.SocketOption
import akka.stream.alpakka.udp.Datagram
import akka.stream.alpakka.udp.impl.{UdpBindFlow, UdpSendFlow}
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Flow

import scala.Option
import scala.collection.immutable.Iterable
import scala.concurrent.Future

object Udp {

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   */
  def sendFlow()(implicit system: ClassicActorSystemProvider): Flow[Datagram, Datagram, NotUsed] =
    sendFlow(system.classicSystem)

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   */
  def sendFlow(system: ActorSystem): Flow[Datagram, Datagram, NotUsed] =
    Flow.fromGraph(new UdpSendFlow(Option.empty)(system))

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   */
  def sendFlow(
      options: Iterable[SocketOption]
  )(implicit system: ClassicActorSystemProvider): Flow[Datagram, Datagram, NotUsed] =
    sendFlow(options, system.classicSystem)

  /**
   * Creates a flow that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message. All incoming messages are also emitted from the flow for
   * subsequent processing.
   */
  def sendFlow(options: Iterable[SocketOption], system: ActorSystem): Flow[Datagram, Datagram, NotUsed] =
    Flow.fromGraph(new UdpSendFlow(Option(options))(system))

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   */
  def sendSink()(implicit system: ClassicActorSystemProvider): Sink[Datagram, NotUsed] = sendFlow().to(Sink.ignore)

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   */
  def sendSink(system: ActorSystem): Sink[Datagram, NotUsed] = sendFlow(system).to(Sink.ignore)

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   */
  def sendSink(options: Iterable[SocketOption])(
      implicit system: ClassicActorSystemProvider
  ): Sink[Datagram, NotUsed] = sendFlow(options).to(Sink.ignore)

  /**
   * Creates a sink that will send all incoming [UdpMessage] messages to the remote address
   * contained in the message.
   */
  def sendSink(options: Iterable[SocketOption], system: ActorSystem): Sink[Datagram, NotUsed] =
    sendFlow(options, system).to(Sink.ignore)

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   */
  def bindFlow(
      localAddress: InetSocketAddress
  )(implicit system: ClassicActorSystemProvider): Flow[Datagram, Datagram, Future[InetSocketAddress]] =
    bindFlow(localAddress, system.classicSystem)

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   */
  def bindFlow(localAddress: InetSocketAddress,
               system: ActorSystem): Flow[Datagram, Datagram, Future[InetSocketAddress]] =
    Flow.fromGraph(new UdpBindFlow(localAddress, Option.empty)(system))

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   */
  def bindFlow(
      localAddress: InetSocketAddress,
      options: Iterable[SocketOption]
  )(implicit system: ClassicActorSystemProvider): Flow[Datagram, Datagram, Future[InetSocketAddress]] =
    bindFlow(localAddress, options, system.classicSystem)

  /**
   * Creates a flow that upon materialization binds to the given `localAddress`. All incoming
   * messages to the `localAddress` are emitted from the flow. All incoming messages to the flow
   * are sent to the remote address contained in the message.
   */
  def bindFlow(localAddress: InetSocketAddress,
               options: Iterable[SocketOption],
               system: ActorSystem): Flow[Datagram, Datagram, Future[InetSocketAddress]] =
    Flow.fromGraph(new UdpBindFlow(localAddress, Option(options))(system))
}
