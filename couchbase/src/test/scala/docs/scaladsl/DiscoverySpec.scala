/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.couchbase.client.java.document.JsonDocument
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class DiscoverySpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val actorSystem: ActorSystem = ActorSystem(
    "with-discovery",
    ConfigFactory.parseString("""
      akka {
        loggers = ["akka.event.slf4j.Slf4jLogger"]
        logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
        loglevel = "DEBUG"
      }

      // #discovery-settings
      alpakka.couchbase {
        session {
          service {
            name = couchbase-service
            lookup-timeout = 1 s
          }
          username = "anotherUser"
          password = "differentPassword"
        }
      }

      akka.discovery.method = config
      akka.discovery.config.services = {
        couchbase-service = {
          endpoints = [
            { host = "akka.io" }
          ]
        }
      }
      // #discovery-settings
     """)
  )
  implicit val mat: Materializer = ActorMaterializer()

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 250.millis)

  val bucketName = "akka"

  override def afterAll(): Unit =
    actorSystem.terminate()

  "a Couchbasesession" should {
    "be managed by the registry" in {
      // #registry
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
      import akka.stream.alpakka.couchbase.{CouchbaseSessionRegistry, CouchbaseSessionSettings, DiscoverySupport}

      val registry = CouchbaseSessionRegistry(actorSystem)

      val sessionSettings = CouchbaseSessionSettings(actorSystem)
        .withEnrichAsync(DiscoverySupport.nodes())
      val sessionFuture: Future[CouchbaseSession] = registry.sessionFor(sessionSettings, bucketName)
      // #registry
      sessionFuture.failed.futureValue shouldBe a[com.couchbase.client.core.config.ConfigurationException]
    }

    "be created from settings" in {
      // #create
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
      import akka.stream.alpakka.couchbase.{CouchbaseSessionSettings, DiscoverySupport}

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
