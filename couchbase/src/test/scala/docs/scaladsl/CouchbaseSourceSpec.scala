/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource
import akka.stream.alpakka.couchbase.testing.CouchbaseSupport
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit._
import com.couchbase.client.java.json.JsonValue
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future
import scala.concurrent.duration._

class CouchbaseSourceSpec
    extends AnyWordSpec
    with BeforeAndAfterAll
    with CouchbaseSupport
    with Matchers
    with ScalaFutures
    with LogCapturing {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 250.millis)

  "CouchbaseSource" should {

    "run simple Query" in assertAllStagesStopped {
      // #statement

      val resultAsFuture: Future[Seq[JsonValue]] =
        CouchbaseSource
          .fromQuery(sessionSettings,
                     bucketName,
                     "SELECT * FROM `" + bucketName + "`.`" + scopeName + "`.`" + collectionName + "` LIMIT 10")
          .runWith(Sink.seq)
      // #statement

      resultAsFuture.futureValue.length shouldEqual 4
    }

  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    upsertSampleData(bucketName, scopeName, collectionName)
  }

  override def afterAll(): Unit = {
    cleanAllInCollection(bucketName, scopeName, collectionName)
    super.afterAll()
  }

}
