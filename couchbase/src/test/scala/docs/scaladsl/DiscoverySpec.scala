/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import com.couchbase.client.java.document.JsonDocument
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class DiscoverySpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures with LogCapturing {

  val config: Config = ConfigFactory.parseResources("discovery.conf")

  implicit val actorSystem: ActorSystem = ActorSystem("DiscoverySpec", config)

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 250.millis)

  val bucketName = "akka"

  override def afterAll(): Unit =
    actorSystem.terminate()

  "a Couchbasesession" should {
    "be managed by the registry" in {
      // #registry
      import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseSession, DiscoverySupport}
      import akka.stream.alpakka.couchbase.{CouchbaseSessionRegistry, CouchbaseSessionSettings}

      val registry = CouchbaseSessionRegistry(actorSystem)

      val sessionSettings = CouchbaseSessionSettings(actorSystem)
        .withEnrichAsync(DiscoverySupport.nodes())
      val sessionFuture: Future[CouchbaseSession] = registry.sessionFor(sessionSettings, bucketName)
      // #registry
      sessionFuture.failed.futureValue shouldBe a[com.couchbase.client.core.config.ConfigurationException]
    }

    "be created from settings" in {
      // #create
      import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
      import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseSession, DiscoverySupport}

      implicit val ec: ExecutionContext = actorSystem.dispatcher
      val sessionSettings = CouchbaseSessionSettings(actorSystem)
        .withEnrichAsync(DiscoverySupport.nodes())
      val sessionFuture: Future[CouchbaseSession] = CouchbaseSession(sessionSettings, bucketName)
      actorSystem.registerOnTermination(sessionFuture.flatMap(_.close()))

      val documentFuture = sessionFuture.flatMap { session =>
        val id = "myId"
        val documentFuture: Future[Option[JsonDocument]] = session.get(id)
        documentFuture.flatMap {
          case Some(jsonDocument) =>
            Future.successful(jsonDocument)
          case None =>
            Future.failed(new RuntimeException(s"document $id wasn't found"))
        }
      }
      // #create
      documentFuture.failed.futureValue shouldBe a[RuntimeException]
    }

  }
}
