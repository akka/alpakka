/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseFlow, CouchbaseSource}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit._
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.dsl.Expression._
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class CouchbaseSourceSpec extends Specification with BeforeAfterAll with CouchbaseSupport with Matchers {

  sequential

  "CouchbaseSource" should {

    "run get by Bulk flow" in assertAllStagesStopped {

      // #by-bulk-id-flow
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow
      import com.couchbase.client.java.document.JsonDocument

      val ids = Seq("First", "Second", "Third", "Fourth")

      val futureResult: Future[Seq[JsonDocument]] =
        Source(ids)
          .via(CouchbaseFlow.fromId(sessionSettings, queryBucketName))
          .runWith(Sink.seq[JsonDocument])
      // #by-bulk-id-flow

      val result = Await.result(futureResult, 5.seconds)

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "run simple Statement Query" in assertAllStagesStopped {
      val resultAsFuture: Future[Seq[JsonObject]] =
        CouchbaseSource
          .fromStatement(sessionSettings, select("*").from(i(queryBucketName)).limit(10), bucketName)
          .runWith(Sink.seq)
      val result = Await.result(resultAsFuture, 5.seconds)
      result.length shouldEqual 4
    }

    "run simple N11QL query" in assertAllStagesStopped {

      //#init-sourcen1ql
      import com.couchbase.client.java.query.{N1qlParams, N1qlQuery}

      val params = N1qlParams.build.adhoc(false)
      val query = N1qlQuery.simple(s"select count(*) from $queryBucketName", params)

      val resultAsFuture: Future[Seq[JsonObject]] =
        CouchbaseSource.fromN1qlQuery(sessionSettings, query, bucketName).runWith(Sink.seq)
      //#init-sourcen1ql

      val result = Await.result(resultAsFuture, 5.seconds)
      result.head.get("$1") shouldEqual 4
    }

  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val bulkUpsertResult: Future[Done] = Source(sampleSequence)
      .map(toJsonDocument)
      .via(CouchbaseFlow.upsert(sessionSettings, CouchbaseWriteSettings.inMemory, queryBucketName))
      .runWith(Sink.ignore)
    Await.result(bulkUpsertResult, 5.seconds)
    //all queries are Eventual Consistent, se we need to wait for index refresh!!
    Thread.sleep(2000)
  }

  override def afterAll(): Unit = {
    cleanAllInBucket(sampleSequence.map(_.id), queryBucketName)
    super.afterAll()
  }

}
