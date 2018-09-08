/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseFlow, CouchbaseSink, CouchbaseSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.couchbase.client.deps.io.netty.buffer.Unpooled
import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.auth.PasswordAuthenticator
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{BinaryDocument, JsonDocument, RawJsonDocument, StringDocument}
import com.couchbase.client.java.{Bucket, CouchbaseCluster}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

trait CouchbaseSupport {

  private val log = LoggerFactory.getLogger(classOf[CouchbaseSupport])

  case class TestObject(id: String, value: String)

  //#init-actor-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  //#init-actor-system

  val head = TestObject("First", "First")

  val bulk: Seq[TestObject] = head +: Seq[TestObject](TestObject("Second", "Second"),
                                                      TestObject("Third", "Third"),
                                                      TestObject("Fourth", "Fourth"))

  val cluster: CouchbaseCluster = CouchbaseCluster.create("localhost")

  cluster.authenticate(new PasswordAuthenticator("Administrator", "password"))

  var akkaBucket: Bucket = _

  var queryBucket: Bucket = _

  def beforeAll(): Unit = {

    log.info("Creating CB Server!!!!")
    try {
      val indexAndQuotaFuture: Future[HttpResponse] = Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://127.0.0.1:8091/pools/default",
          entity = akka.http.scaladsl.model
            .FormData(Map("memoryQuota" -> "300", "indexMemoryQuota" -> "120"))
            .toEntity(HttpCharsets.`UTF-8`),
          protocol = HttpProtocols.`HTTP/1.1`
        )
      )
      log.info(
        s"Creating index and Quota ops result - ${Await.result(indexAndQuotaFuture.map(_.status), Duration(5, TimeUnit.SECONDS)).toString()}"
      )

      val setupServicesFuture = Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://127.0.0.1:8091/node/controller/setupServices",
          entity = akka.http.scaladsl.model.FormData(Map("services" -> "kv,n1ql,index")).toEntity(HttpCharsets.`UTF-8`),
          protocol = HttpProtocols.`HTTP/1.1`
        )
      )

      log.info(
        s"Initiating CB services ops result ${Await.result(setupServicesFuture.map(_.status), Duration(5, TimeUnit.SECONDS))}"
      )

      val credentialsFuture = Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://127.0.0.1:8091/settings/web",
          entity = akka.http.scaladsl.model
            .FormData(Map("port" -> "8091", "username" -> "Administrator", "password" -> "password"))
            .toEntity(HttpCharsets.`UTF-8`),
          protocol = HttpProtocols.`HTTP/1.1`
        )
      )
      log.info(
        s"Setting credentials result - ${Await.result(credentialsFuture.map(_.status), Duration(5, TimeUnit.SECONDS))}"
      )

      val indexServiceFuture = Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://127.0.0.1:8091/settings/indexes",
          entity = akka.http.scaladsl.model.FormData(Map("storageMode" -> "forestdb")).toEntity(HttpCharsets.`UTF-8`),
          protocol = HttpProtocols.`HTTP/1.1`
        ).withHeaders(headers.RawHeader("Authorization", "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="))
      )

      log.info(s"Setting indexing service storage mode ops result - ${Await.result(indexServiceFuture.map(_.status),
                                                                                   Duration(5, TimeUnit.SECONDS))}")

      val bucketN1QLFuture = Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://127.0.0.1:8091/pools/default/buckets",
          entity = akka.http.scaladsl.model
            .FormData(Map("name" -> "akkaquery", "ramQuotaMB" -> "100", "authType" -> "none", "replicaNumber" -> "1"))
            .toEntity(HttpCharsets.`UTF-8`),
          protocol = HttpProtocols.`HTTP/1.1`
        ).withHeaders(headers.RawHeader("Authorization", "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="))
      )

      log.info(
        s"Creating Query Bucket ops result - ${Await.result(bucketN1QLFuture.map(_.status), Duration(5, TimeUnit.SECONDS))}"
      )

      val bucketFuture = Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://127.0.0.1:8091/pools/default/buckets",
          entity = akka.http.scaladsl.model
            .FormData(Map("name" -> "akka", "ramQuotaMB" -> "100", "authType" -> "none", "replicaNumber" -> "1"))
            .toEntity(HttpCharsets.`UTF-8`),
          protocol = HttpProtocols.`HTTP/1.1`
        ).withHeaders(headers.RawHeader("Authorization", "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="))
      )

      log.info(
        s"Creating akka bucket ops result - ${Await.result(bucketFuture.map(_.entity.toString), Duration(5, TimeUnit.SECONDS))}"
      )
    } catch {
      case ex: Throwable => log.warn(ex.getMessage + " Obviously all buckets already created. ")
    }

    Thread.sleep(4000)

    val indexFuture = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = "http://localhost:8093/query/service",
        entity = akka.http.scaladsl.model
          .FormData(Map("statement" -> "CREATE PRIMARY INDEX ON akkaquery USING GSI"))
          .toEntity(HttpCharsets.`UTF-8`),
        protocol = HttpProtocols.`HTTP/1.1`
      ).withHeaders(headers.RawHeader("Authorization", "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="))
    )

    log.info(
      s"Create index on query bucket ops result - ${Await.result(indexFuture.map(_.status), Duration(10, TimeUnit.SECONDS))}"
    )

    log.info("Done Creating CB Server!!!!")

    akkaBucket = cluster.openBucket("akka")
    queryBucket = cluster.openBucket("akkaquery")

  }

  def createCBRawJson(testObject: TestObject): RawJsonDocument = {
    val json = Json.toJson(testObject)(Json.writes[TestObject]).toString()
    RawJsonDocument.create(testObject.id, json)
  }

  def createCBJson(testObject: TestObject): JsonDocument =
    JsonDocument.create(testObject.id, JsonObject.create().put("id", testObject.id).put("value", testObject.value))

  def createCBString(testObject: TestObject): StringDocument = {
    val json = Json.toJson(testObject)(Json.writes[TestObject]).toString()
    StringDocument.create(testObject.id, json)
  }

  def createCBBinary(testObject: TestObject): BinaryDocument = {
    val json = Json.toJson(testObject)(Json.writes[TestObject]).toString()
    val toWrite = Unpooled.copiedBuffer(json, CharsetUtil.UTF_8)
    BinaryDocument.create(testObject.id, toWrite)
  }

  def cleanAllInBucket(ids: Seq[String], bucket: Bucket): Unit = {
    val result: Future[Done] = Source.fromIterator(() => ids.iterator).runWith(CouchbaseSink.deleteOne(2, bucket))
    Await.result(result, Duration.apply(5, TimeUnit.SECONDS))
    ()
  }

  def afterAll(): Unit = {
    system.terminate()
    akkaBucket.close()
    queryBucket.close()
  }

}

private class ConnectToClusterSnippet {
  //#init-cluster
  val cluster: CouchbaseCluster = CouchbaseCluster.create("Some Cluster")
  cluster.authenticate(new PasswordAuthenticator("Some Admin Password", "some password"))
  val bucket: Bucket = cluster.openBucket("akka")
  //#init-cluster
}

private class CBSourceSingleSnippet {

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  //#init-sourceSingle
  import akka.NotUsed
  import com.couchbase.client.java.document.JsonDocument
  val bucket: Bucket = ???

  val id: String = ???
  val source: Source[JsonDocument, NotUsed] = CouchbaseSource[JsonDocument](id, bucket)
  val result: Future[JsonDocument] = source.runWith(Sink.head)
  //#init-sourceSingle
}

private class CBSourceBulkSnippet {

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  //#init-sourceBulk
  import akka.NotUsed
  import com.couchbase.client.java.document.JsonDocument
  //#init-sourceBulk

  val bucket: Bucket = ???

  //#init-sourceBulk
  val ids: Seq[String] = ???
  val source: Source[JsonDocument, NotUsed] = CouchbaseSource[JsonDocument](ids, bucket)
  val result: Future[JsonDocument] = source.runWith(Sink.head)
  //#init-sourceBulk

}

private class CBN1QLQuerySnippet {

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  //#init-sourcen1ql
  import com.couchbase.client.java.query.{AsyncN1qlQueryRow, N1qlParams, N1qlQuery}
  //#init-sourcen1ql

  val bucket: Bucket = ???
  //#init-sourcen1ql
  val params = N1qlParams.build.adhoc(false)
  val query = N1qlQuery.simple("select count(*) from akkaquery", params)

  val resultAsFuture: Future[Seq[AsyncN1qlQueryRow]] = CouchbaseSource(query, bucket).runWith(Sink.seq)
  //#init-sourcen1ql
}

private class UpsertSingleSinkSnippet {

  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  import scala.concurrent.ExecutionContext.Implicits.global

  //#init-SingleSink
  import akka.stream.scaladsl.Source
  import com.couchbase.client.java.document.JsonDocument
  val document: JsonDocument = ???
  val source: Source[JsonDocument, NotUsed] = Source.single(document)
  source.runWith(CouchbaseSink.upsertSingle(2, bucket))

  //#init-SingleSink
}

private class UsertBulkSinkSnippet {

  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  import scala.concurrent.ExecutionContext.Implicits.global

  //#init-BulkSink
  import akka.stream.scaladsl.Source
  import com.couchbase.client.java.document.JsonDocument
  val documents: Seq[JsonDocument] = ???

  val source: Source[JsonDocument, NotUsed] = Source.fromIterator(() => documents.iterator)
  source.grouped(2).runWith(CouchbaseSink.upsertBulk(2, bucket))
  //#init-BulkSink
}

private class DeleteSingleSinkSnippet {

  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  import scala.concurrent.ExecutionContext.Implicits.global

  //#delete-SingleSink
  import akka.stream.scaladsl.Source
  import akka.NotUsed
  val documentId: String = ???
  val source: Source[String, NotUsed] = Source.single(documentId)
  source.runWith(CouchbaseSink.deleteOne(2, bucket))

  //#delete-SingleSink
}

private class DeleteBulkSinkSnippet {

  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  import scala.concurrent.ExecutionContext.Implicits.global

  //#delete-BulkSink
  import akka.stream.scaladsl.Source
  import akka.NotUsed

  val documentIds: Seq[String] = ???
  val source: Source[String, NotUsed] = Source.fromIterator(() => documentIds.iterator)
  source.grouped(2).runWith(CouchbaseSink.deleteBulk(2, bucket))
  //#delete-BulkSink
}

private class DeleteBulkFlowSnippet {

  private val log = LoggerFactory.getLogger(classOf[DeleteBulkFlowSnippet])
  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  import scala.concurrent.ExecutionContext.Implicits.global

  //#delete-BulkFlow
  import akka.stream.scaladsl.Source

  val documentIds: Seq[String] = ???
  val source: Source[String, NotUsed] = Source.fromIterator(() => documentIds.iterator)
  val flow: Flow[Seq[String], (Seq[String], Seq[(String, Throwable)]), NotUsed] = CouchbaseFlow.deleteBulk(2, bucket)

  source
    .grouped(2)
    .via(flow)
    .map(result => {
      result match {
        case (_, errors) => errors.foreach(error => log.error(error._1, error._2))
        case _ =>
      }
      result._1
    })
    .runWith(Sink.ignore)
  //#delete-BulkFlow
}
private class UpsertSingleFlowSnippet {

  private val log = LoggerFactory.getLogger(classOf[DeleteBulkFlowSnippet])
  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  //#init-SingleFlow
  import akka.stream.scaladsl.Source
  import com.couchbase.client.java.document.JsonDocument

  val document: JsonDocument = ???

  val source: Source[JsonDocument, NotUsed] = Source.single(document)

  val flow: Flow[JsonDocument, (JsonDocument, Try[String]), NotUsed] =
    CouchbaseFlow.upsertSingle[JsonDocument](2, bucket)
  source
    .via(flow)
    .map(result => {
      result._2 match {
        case Failure(exception) => log.error(result._1.id, exception)
        case Success(id) => log.info(s"Document with id $id successfully deleted.")
      }
      result._1
    })
    .runWith(Sink.ignore)
  //#init-SingleFlow
}
