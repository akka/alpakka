/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.alpakka.couchbase.testing.CouchbaseSupport
import com.couchbase.client.java.document.JsonDocument
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class CouchbaseSessionExamplesSpec
    extends WordSpec
    with CouchbaseSupport
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 250.millis)

  override def beforeAll(): Unit = super.beforeAll()
  override def afterAll(): Unit = super.afterAll()

  "a Couchbasesession" should {
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

    "be created from a bucket" in {
      implicit val ec: ExecutionContext = actorSystem.dispatcher
      // #fromBucket
      import com.couchbase.client.java.auth.PasswordAuthenticator
      import com.couchbase.client.java.{Bucket, CouchbaseCluster}

      val cluster: CouchbaseCluster = CouchbaseCluster.create("localhost")
      cluster.authenticate(new PasswordAuthenticator("Administrator", "password"))
      val bucket: Bucket = cluster.openBucket("akka")
      val session: CouchbaseSession = CouchbaseSession(bucket)
      actorSystem.registerOnTermination {
        session.close()
        bucket.close()
      }

      val id = "myId"
      val documentFuture = session.get(id).flatMap {
        case Some(jsonDocument) =>
          Future.successful(jsonDocument)
        case None =>
          Future.failed(new RuntimeException(s"document $id wasn't found"))
      }
      // #fromBucket
      documentFuture.failed.futureValue shouldBe a[RuntimeException]
    }
  }
}
