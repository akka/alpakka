/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.alpakka.mqtt.streaming.MqttSessionSettings
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, ActorMqttServerSession}
import org.scalatest.wordspec.AnyWordSpec

class MqttTypedActorSystemSpec extends AnyWordSpec {

  implicit val actorSystem: ActorSystem[Nothing] =
    akka.actor.typed.ActorSystem(Behaviors.ignore, "MqttTypedActorSystemSpec")

  "A typed actor system" should {
    "allow client creation" in {
      val settings = MqttSessionSettings()
      val session = ActorMqttClientSession(settings)
      session.shutdown()
    }

    "allow server creation" in {
      val settings = MqttSessionSettings()
      val session = ActorMqttServerSession(settings)
      session.shutdown()
    }
  }

}

class MqttClassicActorSystemSpec extends AnyWordSpec {

  implicit val actorSystem: actor.ActorSystem = akka.actor.ActorSystem("MqttClassicActorSystemSpec")

  "A typed actor system" should {
    "allow client creation" in {
      val settings = MqttSessionSettings()
      val session = ActorMqttClientSession(settings)
      session.shutdown()
    }

    "allow server creation" in {
      val settings = MqttSessionSettings()
      val session = ActorMqttServerSession(settings)
      session.shutdown()
    }
  }

}
