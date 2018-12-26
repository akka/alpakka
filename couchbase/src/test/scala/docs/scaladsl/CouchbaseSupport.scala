/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings
import akka.stream.alpakka.couchbase.scaladsl._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.couchbase.client.deps.io.netty.buffer.Unpooled
import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.auth.PasswordAuthenticator
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{BinaryDocument, JsonDocument, RawJsonDocument, StringDocument}
import com.couchbase.client.java.{Bucket, CouchbaseCluster, PersistTo, ReplicateTo}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

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

  val couchbaseWriteSettings: CouchbaseWriteSettings = CouchbaseWriteSettings().withReplicateTo(ReplicateTo.NONE)
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
            .FormData(Map("memoryQuota" -> "300", "indexMemoryQuota" -> "256"))
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
    val result: Future[Done] =
      Source.fromIterator(() => ids.iterator).runWith(CouchbaseSink.deleteOne(couchbaseWriteSettings, bucket))
    Await.result(result, Duration.apply(5, TimeUnit.SECONDS))
    ()
  }

  def afterAll(): Unit = {
    system.terminate()
    akkaBucket.close()
    queryBucket.close()
  }

}

private class UpsertSingleSinkSnippet {

  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  //#init-SingleSink
  import akka.stream.scaladsl.Source
  import com.couchbase.client.java.document.JsonDocument
  val document: JsonDocument = ???
  val source: Source[JsonDocument, NotUsed] = Source.single(document)

  val couchbaseWriteSettings: CouchbaseWriteSettings = CouchbaseWriteSettings().withParallelism(2)

  source.runWith(CouchbaseSink.upsertSingle(couchbaseWriteSettings, bucket))

  //#init-SingleSink
}

private class UsertBulkSinkSnippet {

  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  //#init-BulkSink
  import akka.stream.scaladsl.Source
  import com.couchbase.client.java.document.JsonDocument
  val documents: Seq[JsonDocument] = ???

  val couchbaseWriteSettings: CouchbaseWriteSettings =
    CouchbaseWriteSettings().withParallelism(2).withReplicateTo(ReplicateTo.NONE).withPersistTo(PersistTo.NONE)

  val source: Source[JsonDocument, NotUsed] = Source.fromIterator(() => documents.iterator)
  source.grouped(2).runWith(CouchbaseSink.upsertBulk(couchbaseWriteSettings, bucket))
  //#init-BulkSink
}

private class DeleteSingleSinkSnippet {

  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  //#delete-SingleSink
  import akka.NotUsed
  import akka.stream.scaladsl.Source
  val documentId: String = ???
  val source: Source[String, NotUsed] = Source.single(documentId)

  val couchbaseWriteSettings: CouchbaseWriteSettings = CouchbaseWriteSettings()

  source.runWith(CouchbaseSink.deleteOne(couchbaseWriteSettings, bucket))

  //#delete-SingleSink
}

private class DeleteBulkSinkSnippet {

  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  //#delete-BulkSink
  import akka.NotUsed
  import akka.stream.scaladsl.Source

  val documentIds: Seq[String] = ???
  val source: Source[String, NotUsed] = Source.fromIterator(() => documentIds.iterator)

  val couchbaseWriteSettings: CouchbaseWriteSettings =
    CouchbaseWriteSettings().withParallelism(2).withReplicateTo(ReplicateTo.NONE).withPersistTo(PersistTo.NONE)

  source.grouped(2).runWith(CouchbaseSink.deleteBulk(couchbaseWriteSettings, bucket))
  //#delete-BulkSink
}

private class DeleteBulkFlowSnippet {

  private val log = LoggerFactory.getLogger(classOf[DeleteBulkFlowSnippet])
  val bucket: Bucket = ???
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  //#delete-BulkFlow
  import akka.stream.scaladsl.Source
  import akka.stream.alpakka.couchbase.BulkOperationResult

  val documentIds: Seq[String] = ???
  val source: Source[String, NotUsed] = Source.fromIterator(() => documentIds.iterator)

  val couchbaseWriteSettings: CouchbaseWriteSettings =
    CouchbaseWriteSettings().withParallelism(2).withReplicateTo(ReplicateTo.NONE).withPersistTo(PersistTo.NONE)

  val flow: Flow[Seq[String], BulkOperationResult[String], NotUsed] =
    CouchbaseFlow.deleteBulk(couchbaseWriteSettings, bucket)

  source
    .grouped(2)
    .via(flow)
    .map(result => {
      //Logging failed operation
      result.failures.foreach(f => log.warn(f.id, f.ex))
      result.entities
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
  import akka.stream.alpakka.couchbase.SingleOperationResult
  val document: JsonDocument = ???

  val source: Source[JsonDocument, NotUsed] = Source.single(document)

  val couchbaseWriteSettings: CouchbaseWriteSettings =
    CouchbaseWriteSettings().withParallelism(2).withReplicateTo(ReplicateTo.NONE).withPersistTo(PersistTo.NONE)

  val flow: Flow[JsonDocument, SingleOperationResult[JsonDocument], NotUsed] =
    CouchbaseFlow.upsertSingle[JsonDocument](couchbaseWriteSettings, bucket)
  source
    .via(flow)
    .map(response => {
      response.result match {
        case Failure(exception) => log.error(response.entity.id(), exception)
        case Success(id) => log.info(s"Document with id $id successfully deleted.")
      }
      response.entity
    })
    .runWith(Sink.ignore)
  //#init-SingleFlow
}
