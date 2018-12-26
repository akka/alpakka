/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseFlow, CouchbaseSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.couchbase.client.java.PersistTo
import com.couchbase.client.java.query.AsyncN1qlQueryRow
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.dsl.Expression._
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import akka.stream.testkit.scaladsl.StreamTestKit._

import scala.collection.immutable.Seq
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CouchbaseSourceSpec extends Specification with BeforeAfterAll with CouchbaseSupport with Matchers {

  sequential

  "CouchbaseSource" should {

    "initiate Couchbase Cluster" in {

      //#init-cluster
      import com.couchbase.client.java.{Bucket, CouchbaseCluster}
      import com.couchbase.client.java.auth.PasswordAuthenticator

      val cluster: CouchbaseCluster = CouchbaseCluster.create("localhost")
      cluster.authenticate(new PasswordAuthenticator("Administrator", "password"))
      val cbBucket: Bucket = cluster.openBucket("akka")
      //#init-cluster

      val isClosed: Boolean = cbBucket.isClosed
      cbBucket.close()

      isClosed shouldEqual false
    }

    "run get by Bulk flow" in assertAllStagesStopped {

      // #by-bulk-id-flow
      import akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow
      import com.couchbase.client.java.document.JsonDocument

      val ids = Seq("First", "Second", "Third", "Fourth")

      val source: Source[Seq[String], NotUsed] = Source.single(ids)
      val flow: Flow[Seq[String], Seq[JsonDocument], NotUsed] =
        CouchbaseFlow.fromBulkIds(queryBucket, classOf[JsonDocument])
      val futureResult: Future[Seq[JsonDocument]] = source.via(flow).runWith(Sink.head[Seq[JsonDocument]])
      // #by-bulk-id-flow

      val result = Await.result(futureResult, Duration(5, TimeUnit.SECONDS))

      result.map(_.id()) must contain(exactly("First", "Second", "Third", "Fourth"))
    }

    "run simple Statement Query" in assertAllStagesStopped {
      val resultAsFuture: Future[Seq[AsyncN1qlQueryRow]] =
        CouchbaseSource.fromStatement(select("*").from(i("akkaquery")).limit(10), queryBucket).runWith(Sink.seq)
      val result = Await.result(resultAsFuture, Duration(5, TimeUnit.SECONDS))
      result.length shouldEqual 4
    }

    "run simple N11QL query" in assertAllStagesStopped {

      //#init-sourcen1ql
      import com.couchbase.client.java.query.{AsyncN1qlQueryRow, N1qlParams, N1qlQuery}

      val params = N1qlParams.build.adhoc(false)
      val query = N1qlQuery.simple("select count(*) from akkaquery", params)

      val resultAsFuture: Future[Seq[AsyncN1qlQueryRow]] =
        CouchbaseSource.fromN1qlQuery(query, queryBucket).runWith(Sink.seq)
      //#init-sourcen1ql

      val result = Await.result(resultAsFuture, Duration(5, TimeUnit.SECONDS))
      result.head.value().get("$1") shouldEqual 4
    }

  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val bulkUpsertResult: Future[Done] = Source
      .fromIterator(() => bulk.iterator)
      .map(createCBJson)
      .grouped(2)
      .via(CouchbaseFlow.upsertBulk(CouchbaseWriteSettings().withPersistTo(PersistTo.NONE), queryBucket))
      .runWith(Sink.ignore)
    Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))
    //all queries are Eventual Consistent, se we need to wait for index refresh!!
    Thread.sleep(2000)
  }

  override def afterAll(): Unit =
    //cleanAllInBucket(bulk.map(_.id),queryBucket)
    super.afterAll()

}
