/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming.scaladsl.{
  ActorMqttClientSession => ScalaActorMqttClientSession,
  ActorMqttServerSession => ScalaActorMqttServerSession,
  MqttClientSession => ScalaMqttClientSession,
  MqttServerSession => ScalaMqttServerSession
}

import scala.compat.java8.FutureConverters._

/**
 * Represents MQTT session state for both clients or servers. Session
 * state can survive across connections i.e. their lifetime is
 * generally longer.
 */
abstract class MqttSession {

  /**
   * Stop the session
   * @return [[Done]] when complete
   */
  def stop(): CompletionStage[Done]
}

/**
 * Represents client-only sessions
 */
abstract class MqttClientSession extends MqttSession {
  protected[javadsl] val underlying: ScalaMqttClientSession

  override def stop(): CompletionStage[Done] =
    underlying.stop().toJava
}

/**
 * Provides an actor implementation of a client session
 *
 * @param settings session settings
 */
final class ActorMqttClientSession(settings: MqttSessionSettings, system: ActorSystem) extends MqttClientSession {
  override protected[javadsl] val underlying: ScalaActorMqttClientSession =
    ScalaActorMqttClientSession(settings)(system)
}

/**
 * Represents server-only sessions
 */
abstract class MqttServerSession extends MqttSession {
  protected[javadsl] val underlying: ScalaMqttServerSession

  override def stop(): CompletionStage[Done] =
    underlying.stop().toJava
}

/**
 * Provides an actor implementation of a server session
 *
 * @param settings session settings
 */
final class ActorMqttServerSession(settings: MqttSessionSettings, system: ActorSystem) extends MqttServerSession {
  override protected[javadsl] val underlying: ScalaActorMqttServerSession =
    ScalaActorMqttServerSession(settings)(system)
}
