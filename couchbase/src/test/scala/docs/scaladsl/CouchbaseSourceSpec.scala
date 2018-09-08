/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.Done
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseFlow, CouchbaseSource}
import akka.stream.scaladsl.{Sink, Source}
import com.couchbase.client.java.query.{AsyncN1qlQueryRow, N1qlParams, N1qlQuery}
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.dsl.Expression._
import org.slf4j.LoggerFactory
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CouchbaseSourceSpec extends Specification with BeforeAfterAll with CouchbaseSupport with Matchers {

  sequential

  "CouchbaseSource" should {

    "run simple Statement Query" in {
      val resultAsFuture: Future[immutable.Seq[AsyncN1qlQueryRow]] =
        CouchbaseSource(select("*").from(i("akkaquery")).limit(10), queryBucket).runWith(Sink.seq)
      val result = Await.result(resultAsFuture, Duration(5, TimeUnit.SECONDS))
      result.length shouldEqual 4
    }

    "run simple N11QL query" in {
      val params = N1qlParams.build.adhoc(false)
      val query = N1qlQuery.simple("select count(*) from akkaquery", params)
      val resultAsFuture: Future[immutable.Seq[AsyncN1qlQueryRow]] =
        CouchbaseSource(query, queryBucket).runWith(Sink.seq)
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
      .via(CouchbaseFlow.upsertBulk(2, queryBucket))
      .runWith(Sink.ignore)
    Await.result(bulkUpsertResult, Duration(5, TimeUnit.SECONDS))
    //all queries are Eventual Consistent, se we need to wait for index refresh!!
    Thread.sleep(1000)
  }

  override def afterAll(): Unit =
    //cleanAllInBucket(bulk.map(_.id),queryBucket)
    super.afterAll()

}
