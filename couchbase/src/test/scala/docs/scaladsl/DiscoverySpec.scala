/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.couchbase.testing.CouchbaseSupport
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class DiscoverySpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures with LogCapturing {

  val config: Config = ConfigFactory.parseResources("discovery.conf")
  private val support = new CouchbaseSupport {}

  implicit val actorSystem: ActorSystem = ActorSystem("DiscoverySpec", config)

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 250.millis)

  override def afterAll(): Unit =
    actorSystem.terminate()

  override def beforeAll(): Unit = super.beforeAll()

  "a Couchbasesession" should {
    "be managed by the registry" in {
      // #registry
      import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseSession, DiscoverySupport}
      import akka.stream.alpakka.couchbase.{CouchbaseSessionRegistry, CouchbaseSessionSettings}

      val registry = CouchbaseSessionRegistry(actorSystem)

      val sessionSettings = CouchbaseSessionSettings(actorSystem)
        .withEnrichAsync(DiscoverySupport.nodes())
      val sessionFuture: Future[CouchbaseSession] = registry.sessionFor(sessionSettings, support.bucketName)
      // #registry
      sessionFuture.futureValue shouldBe a[CouchbaseSession]
    }

    "be created from settings" in {
      // #create
      import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
      import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseSession, DiscoverySupport}

      implicit val ec: ExecutionContext = actorSystem.dispatcher
      val sessionSettings = CouchbaseSessionSettings(actorSystem)
        .withEnrichAsync(DiscoverySupport.nodes())
      val sessionFuture: Future[CouchbaseSession] = CouchbaseSession(sessionSettings, support.bucketName)
      actorSystem.registerOnTermination(sessionFuture.flatMap(_.close()))

      val documentFuture = sessionFuture.flatMap { session =>
        val id = "myId"
        session.collection(support.scopeName, support.collectionName).getBytes(id)
      }
      // #create
      documentFuture.failed.futureValue shouldBe a[RuntimeException]
    }

  }
}
