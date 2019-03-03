/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.alpakka.ironmq.impl.IronMqClient
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3

abstract class IronMqSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterEach {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 15.seconds, interval = 1.second)
  val DefaultActorSystemTerminateTimeout: Duration = 10.seconds

  private implicit val ec = ExecutionContext.global
  private var mutableIronMqClient = Option.empty[IronMqClient]

  private var mutableConfig = Option.empty[Config]
  def config: Config = mutableConfig.getOrElse(throw new IllegalStateException("Config not initialized"))

  protected def initConfig(): Config =
    ConfigFactory.parseString(s"""alpakka.ironmq {
                                 |  credentials {
                                 |    project-id = "${MurmurHash3.stringHash(System.currentTimeMillis().toString)}"
                                 |  }
                                 |}
      """.stripMargin).withFallback(ConfigFactory.load())

  /**
   * Override to tune the time the test will wait for the actor system to terminate.
   */
  def actorSystemTerminateTimeout: Duration = DefaultActorSystemTerminateTimeout

  private var mutableActorSystem = Option.empty[ActorSystem]
  private var mutableMaterializer = Option.empty[Materializer]

  implicit def actorSystem: ActorSystem =
    mutableActorSystem.getOrElse(throw new IllegalArgumentException("The ActorSystem is not initialized"))
  implicit def materializer: Materializer =
    mutableMaterializer.getOrElse(throw new IllegalStateException("Materializer not initialized"))

  def ironMqClient: IronMqClient =
    mutableIronMqClient.getOrElse(throw new IllegalStateException("The IronMqClient is not initialized"))

  override protected def beforeEach(): Unit = {
    mutableConfig = Option(initConfig())
    mutableActorSystem = Option(ActorSystem(s"test-${System.currentTimeMillis()}", config))
    mutableMaterializer = Option(ActorMaterializer())
    mutableIronMqClient = Option(IronMqClient(IronMqSettings(config.getConfig("alpakka.ironmq"))))
  }

  override protected def afterEach(): Unit = {
    mutableIronMqClient = Option.empty
    Await.result(actorSystem.terminate(), actorSystemTerminateTimeout)
  }

  def givenQueue(name: String): String =
    ironMqClient.createQueue(name).futureValue

  def givenQueue(): String =
    givenQueue(s"test-${UUID.randomUUID()}")

}
