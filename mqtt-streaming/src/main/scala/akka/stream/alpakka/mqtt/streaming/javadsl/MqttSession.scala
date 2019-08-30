/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.mqtt.streaming.scaladsl.{
  ActorMqttClientSession => ScalaActorMqttClientSession,
  ActorMqttServerSession => ScalaActorMqttServerSession,
  MqttClientSession => ScalaMqttClientSession,
  MqttServerSession => ScalaMqttServerSession
}
import akka.stream.javadsl.Source

import scala.compat.java8.FutureConverters._

/**
 * Represents MQTT session state for both clients or servers. Session
 * state can survive across connections i.e. their lifetime is
 * generally longer.
 */
abstract class MqttSession {

  /**
   * Tell the session to perform a command regardless of the state it is
   * in. This is important for sending Publish messages in particular,
   * as a connection may not have been established with a session.
   * @param cp The command to perform
   * @tparam A The type of any carry for the command.
   */
  def tell[A](cp: Command[A]): Unit

  /**
   * Ask the session to perform a command regardless of the state it is
   * in. This is important for sending Publish messages in particular,
   * as a connection may not have been established with a session.
   * @param cp The command to perform
   * @tparam A The type of any carry for the command.
   * @return A future indicating when the command has completed. Completion
   *         is defined as when it has been acknowledged by the recipient
   *         endpoint.
   */
  def ask[A](cp: Command[A]): CompletionStage[A]

  /**
   * Shutdown the session gracefully
   */
  def shutdown(): Unit
}

/**
 * Represents client-only sessions
 */
abstract class MqttClientSession extends MqttSession {
  protected[javadsl] val underlying: ScalaMqttClientSession

  override def tell[A](cp: Command[A]): Unit =
    underlying ! cp

  override def ask[A](cp: Command[A]): CompletionStage[A] =
    (underlying ? cp).toJava

  override def shutdown(): Unit =
    underlying.shutdown()
}

object ActorMqttClientSession {
  def create(settings: MqttSessionSettings, mat: Materializer, system: ActorSystem): ActorMqttClientSession =
    new ActorMqttClientSession(settings, mat, system)
}

/**
 * Provides an actor implementation of a client session
 *
 * @param settings session settings
 */
final class ActorMqttClientSession(settings: MqttSessionSettings, mat: Materializer, system: ActorSystem)
    extends MqttClientSession {
  override protected[javadsl] val underlying: ScalaActorMqttClientSession =
    ScalaActorMqttClientSession(settings)(mat, system)
}

object MqttServerSession {

  /**
   * Used to signal that a client session has ended
   */
  final case class ClientSessionTerminated(clientId: String)
}

/**
 * Represents server-only sessions
 */
abstract class MqttServerSession extends MqttSession {
  import MqttServerSession._

  protected[javadsl] val underlying: ScalaMqttServerSession

  /**
   * Used to observe client connections being terminated
   */
  def watchClientSessions: Source[ClientSessionTerminated, NotUsed]

  override def tell[A](cp: Command[A]): Unit =
    underlying ! cp

  override def ask[A](cp: Command[A]): CompletionStage[A] =
    (underlying ? cp).toJava

  override def shutdown(): Unit =
    underlying.shutdown()
}

object ActorMqttServerSession {
  def create(settings: MqttSessionSettings, mat: Materializer, system: ActorSystem): ActorMqttServerSession =
    new ActorMqttServerSession(settings, mat, system)
}

/**
 * Provides an actor implementation of a server session
 *
 * @param settings session settings
 */
final class ActorMqttServerSession(settings: MqttSessionSettings, mat: Materializer, system: ActorSystem)
    extends MqttServerSession {
  import MqttServerSession._

  override protected[javadsl] val underlying: ScalaActorMqttServerSession =
    ScalaActorMqttServerSession(settings)(mat, system)

  override def watchClientSessions: Source[ClientSessionTerminated, NotUsed] =
    underlying.watchClientSessions.map {
      case ScalaMqttServerSession.ClientSessionTerminated(clientId) => ClientSessionTerminated(clientId)
    }.asJava
}
