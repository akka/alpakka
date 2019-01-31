/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.impl
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.alpakka.ironmq.{IronMqSettings, Queue}
import akka.stream.{ActorMaterializer, Materializer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.hashing.MurmurHash3

trait IronMqClientForTests {

  implicit def system: ActorSystem = ActorSystem()
  implicit def materializer: Materializer = ActorMaterializer()
  implicit lazy val executionContext: ExecutionContext = system.dispatcher

  val projectId = s"""${MurmurHash3.stringHash(System.currentTimeMillis().toString)}"""

  val ironMqSettings: IronMqSettings = IronMqSettings(system.settings.config.getConfig(IronMqSettings.ConfigPath))
    .withProjectId(projectId)

  val ironMqClient = IronMqClient(ironMqSettings)

  def givenQueue(name: Queue.Name): Future[Queue] =
    ironMqClient.createQueue(name)

  def givenQueue(): Future[Queue] =
    givenQueue(Queue.Name(s"test-${UUID.randomUUID()}"))

}

class IronMqClientForJava(_system: ActorSystem, _mat: Materializer) extends IronMqClientForTests {
  override implicit def system: ActorSystem = _system
  override implicit def materializer: Materializer = _mat

}
