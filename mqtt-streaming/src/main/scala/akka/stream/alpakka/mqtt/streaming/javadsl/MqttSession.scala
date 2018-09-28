/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming
package javadsl

import akka.stream.alpakka.mqtt.streaming.scaladsl.{
  MqttClientSession => ScalaMqttClientSession,
  ActorMqttClientSession => ScalaActorMqttClientSession,
  MqttServerSession => ScalaMqttServerSession,
  ActorMqttServerSession => ScalaActorMqttServerSession
}

/**
 * Represents MQTT session state for both clients or servers. Session
 * state can survive across connections i.e. their lifetime is
 * generally longer.
 */
abstract class MqttSession

/**
 * Represents client-only sessions
 */
abstract class MqttClientSession extends MqttSession {
  private[javadsl] val underlying: ScalaMqttClientSession
}

/**
 * Provides an actor implementation of a client session
 *
 * @param settings session settings
 */
final class ActorMqttClientSession(settings: MqttSessionSettings) extends MqttClientSession {
  override val underlying = ScalaActorMqttClientSession(settings)
}

/**
 * Represents server-only sessions
 */
abstract class MqttServerSession extends MqttSession {
  private[javadsl] val underlying: ScalaMqttServerSession
}

/**
 * Provides an actor implementation of a server session
 *
 * @param settings session settings
 */
final class ActorMqttServerSession(settings: MqttSessionSettings) extends MqttServerSession {
  override val underlying = ScalaActorMqttServerSession(settings)
}
