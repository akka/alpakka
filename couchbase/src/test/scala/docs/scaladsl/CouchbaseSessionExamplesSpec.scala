/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.stream.alpakka.couchbase.testing.CouchbaseSupport
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.java.ClusterOptions
import com.couchbase.client.java.env.ClusterEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class CouchbaseSessionExamplesSpec
    extends AnyWordSpec
    with CouchbaseSupport
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with LogCapturing {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 250.millis)

  override def beforeAll(): Unit = {
    super.beforeAll()
    upsertSampleData(bucketName, scopeName, collectionName)
  }

  override def afterAll(): Unit = {
    cleanAllInCollection(bucketName, scopeName, collectionName)
    super.afterAll()
  }
  "a Couchbasesession" should {
    "be managed by the registry" in {
      // #registry
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
      import akka.stream.alpakka.couchbase.{CouchbaseSessionRegistry, CouchbaseSessionSettings}

      // Akka extension (singleton per actor system)
      val registry = CouchbaseSessionRegistry(actorSystem)

      // If connecting to more than one Couchbase cluster, the environment should be shared
      val environment: ClusterEnvironment = ClusterEnvironment.create()
      actorSystem.registerOnTermination {
        environment.shutdown()
      }

      val sessionSettings = CouchbaseSessionSettings(actorSystem)
        .withEnvironment(environment)
      val sessionFuture: Future[CouchbaseSession] = registry.sessionFor(sessionSettings, bucketName)
      // #registry
      sessionFuture.futureValue shouldBe a[CouchbaseSession]
    }

    "be created from settings" in {
      // #create
      import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession

      implicit val ec: ExecutionContext = actorSystem.dispatcher
      val sessionSettings = CouchbaseSessionSettings(actorSystem)
      val sessionFuture: Future[CouchbaseSession] = CouchbaseSession(sessionSettings, bucketName)
      actorSystem.registerOnTermination(sessionFuture.flatMap(_.close()))

      val documentFuture = sessionFuture.flatMap { session =>
        val id = "myId"
        session.collection(scopeName, collectionName).getBytes(id)
      }
      // #create
      documentFuture.failed.futureValue.getCause shouldBe a[DocumentNotFoundException]
    }

    "be created from an AsyncCluster" in {
      // #fromCluster
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
      import com.couchbase.client.java.Cluster

      val cluster: Cluster = Cluster.connect("localhost",
                                             ClusterOptions.clusterOptions(
                                               "Administrator",
                                               "password"
                                             ))
      val session: CouchbaseSession = CouchbaseSession(cluster.async(), "akka").futureValue
      actorSystem.registerOnTermination {
        session.close()
      }

      val id = "myId"
      val documentFuture = session.collection(scopeName, collectionName).getBytes(id)
      // #fromCluster
      documentFuture.failed.futureValue.getCause shouldBe a[DocumentNotFoundException]
    }
  }
}
