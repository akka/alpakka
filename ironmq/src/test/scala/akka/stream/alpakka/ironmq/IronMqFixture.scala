/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import java.util.UUID

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, Notifying, Suite}

import scala.concurrent.ExecutionContext
import scala.util.hashing.MurmurHash3

trait IronMqFixture extends AkkaStreamFixture with BeforeAndAfterEach with ScalaFutures { _: Suite with Notifying =>

  private implicit val ec = ExecutionContext.global
  private var mutableIronMqClient = Option.empty[IronMqClient]

  def ironMqClient: IronMqClient =
    mutableIronMqClient.getOrElse(throw new IllegalStateException("The IronMqClient is not initialized"))

  override protected def initConfig(): Config =
    ConfigFactory.parseString(s"""akka.stream.alpakka.ironmq {
        |  credentials {
        |    project-id = "${MurmurHash3.stringHash(System.currentTimeMillis().toString)}"
        |  }
        |}
      """.stripMargin).withFallback(super.initConfig())

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    mutableIronMqClient = Option(IronMqClient(IronMqSettings(config.getConfig("akka.stream.alpakka.ironmq"))))
  }

  override protected def afterEach(): Unit = {
    mutableIronMqClient = Option.empty
    super.afterEach()
  }

  def givenQueue(name: Queue.Name): Queue = {
    val created = ironMqClient.createQueue(name).futureValue
    note(s"Queue created: ${created.name.value}", Some(created))
    created
  }

  def givenQueue(): Queue =
    givenQueue(Queue.Name(s"test-${UUID.randomUUID()}"))
}
