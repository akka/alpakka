/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.scaladsl._
import akka.stream.alpakka.couchbase.{CouchbaseSessionSettings, CouchbaseWriteSettings}
import akka.stream.scaladsl.Source
import com.couchbase.client.deps.io.netty.buffer.Unpooled
import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.auth.PasswordAuthenticator
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{BinaryDocument, JsonDocument, RawJsonDocument, StringDocument}
import com.couchbase.client.java.{CouchbaseCluster, ReplicateTo}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait CouchbaseSupport {

  private val log = LoggerFactory.getLogger(classOf[CouchbaseSupport])

  case class TestObject(id: String, value: String)

  //#init-actor-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  //#init-actor-system

  val sampleData = TestObject("First", "First")

  val sampleSequence: Seq[TestObject] = sampleData +: Seq[TestObject](TestObject("Second", "Second"),
                                                                      TestObject("Third", "Third"),
                                                                      TestObject("Fourth", "Fourth"))

  val cluster: CouchbaseCluster = CouchbaseCluster.create("localhost")

  cluster.authenticate(new PasswordAuthenticator("Administrator", "password"))

  val sessionSettings = CouchbaseSessionSettings(system)
  val writeSettings: CouchbaseWriteSettings = CouchbaseWriteSettings().withReplicateTo(ReplicateTo.NONE)
  val bucketName = "akka"
  val queryBucketName = "akkaquery"

  def beforeAll(): Unit = {
    def setupIndexAndQuota =
      Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://127.0.0.1:8091/pools/default",
          entity = akka.http.scaladsl.model
            .FormData(Map("memoryQuota" -> "300", "indexMemoryQuota" -> "256"))
            .toEntity(HttpCharsets.`UTF-8`),
          protocol = HttpProtocols.`HTTP/1.1`
        )
      )

    def setupServices =
      Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "http://127.0.0.1:8091/node/controller/setupServices",
          entity = akka.http.scaladsl.model.FormData(Map("services" -> "kv,n1ql,index")).toEntity(HttpCharsets.`UTF-8`),
          protocol = HttpProtocols.`HTTP/1.1`
        )
      )

    def setupCredentials = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = "http://127.0.0.1:8091/settings/web",
        entity = akka.http.scaladsl.model
          .FormData(Map("port" -> "8091", "username" -> "Administrator", "password" -> "password"))
          .toEntity(HttpCharsets.`UTF-8`),
        protocol = HttpProtocols.`HTTP/1.1`
      )
    )

    def setupIndexService = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = "http://127.0.0.1:8091/settings/indexes",
        entity = akka.http.scaladsl.model.FormData(Map("storageMode" -> "forestdb")).toEntity(HttpCharsets.`UTF-8`),
        protocol = HttpProtocols.`HTTP/1.1`
      ).withHeaders(headers.RawHeader("Authorization", "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="))
    )

    def setupQueryBucket = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = "http://127.0.0.1:8091/pools/default/buckets",
        entity = akka.http.scaladsl.model
          .FormData(
            Map("name" -> queryBucketName, "ramQuotaMB" -> "100", "authType" -> "none", "replicaNumber" -> "1")
          )
          .toEntity(HttpCharsets.`UTF-8`),
        protocol = HttpProtocols.`HTTP/1.1`
      ).withHeaders(headers.RawHeader("Authorization", "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="))
    )

    def setupBucket = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = "http://127.0.0.1:8091/pools/default/buckets",
        entity = akka.http.scaladsl.model
          .FormData(Map("name" -> bucketName, "ramQuotaMB" -> "100", "authType" -> "none", "replicaNumber" -> "1"))
          .toEntity(HttpCharsets.`UTF-8`),
        protocol = HttpProtocols.`HTTP/1.1`
      ).withHeaders(headers.RawHeader("Authorization", "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="))
    )

    def createIndex = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = "http://localhost:8093/query/service",
        entity = akka.http.scaladsl.model
          .FormData(Map("statement" -> s"CREATE PRIMARY INDEX ON $queryBucketName USING GSI"))
          .toEntity(HttpCharsets.`UTF-8`),
        protocol = HttpProtocols.`HTTP/1.1`
      ).withHeaders(headers.RawHeader("Authorization", "Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=="))
    )

    log.info("Creating CB Server")
    try {
      Await.result(Future.sequence(
                     Seq(
                       setupIndexAndQuota,
                       setupServices,
                       setupCredentials,
                       setupIndexService,
                       setupQueryBucket,
                       setupBucket
                     )
                   ),
                   5.seconds)
    } catch {
      case ex: Throwable => log.warn(ex.getMessage + " Obviously all buckets already created. ")
    }

    Thread.sleep(4000)
    Await.result(createIndex, 5.seconds)
    Thread.sleep(4000)

    log.info("Done Creating CB Server")
  }

  def toRawJsonDocument(testObject: TestObject): RawJsonDocument = {
    val json = Json.toJson(testObject)(Json.writes[TestObject]).toString()
    RawJsonDocument.create(testObject.id, json)
  }

  def toJsonDocument(testObject: TestObject): JsonDocument =
    JsonDocument.create(testObject.id, JsonObject.create().put("id", testObject.id).put("value", testObject.value))

  def toStringDocument(testObject: TestObject): StringDocument = {
    val json = Json.toJson(testObject)(Json.writes[TestObject]).toString()
    StringDocument.create(testObject.id, json)
  }

  def toBinaryDocument(testObject: TestObject): BinaryDocument = {
    val json = Json.toJson(testObject)(Json.writes[TestObject]).toString()
    val toWrite = Unpooled.copiedBuffer(json, CharsetUtil.UTF_8)
    BinaryDocument.create(testObject.id, toWrite)
  }

  def cleanAllInBucket(ids: Seq[String], bucketName: String): Unit = {
    val result: Future[Done] =
      Source(ids)
        .runWith(CouchbaseSink.delete(sessionSettings, CouchbaseWriteSettings.inMemory, bucketName))
    Await.result(result, 5.seconds)
    ()
  }

  def afterAll(): Unit =
    system.terminate()
}

final class CouchbaseSupportClass extends CouchbaseSupport
//
//private class UpsertSingleSinkSnippet {
//
//  val bucket: Bucket = ???
//  implicit val system: ActorSystem = ActorSystem()
//  implicit val mat: ActorMaterializer = ActorMaterializer()
//
//  //#init-SingleSink
//  import akka.stream.scaladsl.Source
//  import com.couchbase.client.java.document.JsonDocument
//  val document: JsonDocument = ???
//  val source: Source[JsonDocument, NotUsed] = Source.single(document)
//
//  val couchbaseWriteSettings: CouchbaseWriteSettings = CouchbaseWriteSettings().withParallelism(2)
//
//  source.runWith(CouchbaseSink.upsertSingle(couchbaseWriteSettings, bucket))
//
//  //#init-SingleSink
//}
//
//private class UsertBulkSinkSnippet {
//
//  val bucket: Bucket = ???
//  implicit val system: ActorSystem = ActorSystem()
//  implicit val mat: ActorMaterializer = ActorMaterializer()
//
//  //#init-BulkSink
//  import akka.stream.scaladsl.Source
//  import com.couchbase.client.java.document.JsonDocument
//  val documents: Seq[JsonDocument] = ???
//
//  val couchbaseWriteSettings: CouchbaseWriteSettings =
//    CouchbaseWriteSettings().withParallelism(2).withReplicateTo(ReplicateTo.NONE).withPersistTo(PersistTo.NONE)
//
//  val source: Source[JsonDocument, NotUsed] = Source.fromIterator(() => documents.iterator)
//  source.grouped(2).runWith(CouchbaseSink.upsertBulk(couchbaseWriteSettings, bucket))
//  //#init-BulkSink
//}
//
//private class DeleteSingleSinkSnippet {
//
//  val bucket: Bucket = ???
//  implicit val system: ActorSystem = ActorSystem()
//  implicit val mat: ActorMaterializer = ActorMaterializer()
//
//  //#delete-SingleSink
//  import akka.NotUsed
//  import akka.stream.scaladsl.Source
//  val documentId: String = ???
//  val source: Source[String, NotUsed] = Source.single(documentId)
//
//  val couchbaseWriteSettings: CouchbaseWriteSettings = CouchbaseWriteSettings()
//
//  source.runWith(CouchbaseSink.deleteOne(couchbaseWriteSettings, bucket))
//
//  //#delete-SingleSink
//}
//
//private class DeleteBulkSinkSnippet {
//
//  val bucket: Bucket = ???
//  implicit val system: ActorSystem = ActorSystem()
//  implicit val mat: ActorMaterializer = ActorMaterializer()
//
//  //#delete-BulkSink
//  import akka.NotUsed
//  import akka.stream.scaladsl.Source
//
//  val documentIds: Seq[String] = ???
//  val source: Source[String, NotUsed] = Source.fromIterator(() => documentIds.iterator)
//
//  val couchbaseWriteSettings: CouchbaseWriteSettings =
//    CouchbaseWriteSettings().withParallelism(2).withReplicateTo(ReplicateTo.NONE).withPersistTo(PersistTo.NONE)
//
//  source.grouped(2).runWith(CouchbaseSink.deleteBulk(couchbaseWriteSettings, bucket))
//  //#delete-BulkSink
//}
//
//private class DeleteBulkFlowSnippet {
//
//  private val log = LoggerFactory.getLogger(classOf[DeleteBulkFlowSnippet])
//  val bucket: Bucket = ???
//  implicit val system: ActorSystem = ActorSystem()
//  implicit val mat: ActorMaterializer = ActorMaterializer()
//
//  //#delete-BulkFlow
//  import akka.stream.scaladsl.Source
//  import akka.stream.alpakka.couchbase.BulkOperationResult
//
//  val documentIds: Seq[String] = ???
//  val source: Source[String, NotUsed] = Source.fromIterator(() => documentIds.iterator)
//
//  val couchbaseWriteSettings: CouchbaseWriteSettings =
//    CouchbaseWriteSettings().withParallelism(2).withReplicateTo(ReplicateTo.NONE).withPersistTo(PersistTo.NONE)
//
//  val flow: Flow[Seq[String], BulkOperationResult[String], NotUsed] =
//    CouchbaseFlow2.deleteOne(sessionSettings, couchbaseWriteSettings, bucketName)
//
//  source
//    .grouped(2)
//    .via(flow)
//    .map(result => {
//      //Logging failed operation
//      result.failures.foreach(f => log.warn(f.id, f.ex))
//      result.entities
//    })
//    .runWith(Sink.ignore)
//  //#delete-BulkFlow
//}
//private class UpsertSingleFlowSnippet {
//
//  private val log = LoggerFactory.getLogger(classOf[DeleteBulkFlowSnippet])
//  val bucket: Bucket = ???
//  implicit val system: ActorSystem = ActorSystem()
//  implicit val mat: ActorMaterializer = ActorMaterializer()
//
//  //#init-SingleFlow
//  import akka.stream.scaladsl.Source
//  import com.couchbase.client.java.document.JsonDocument
//  import akka.stream.alpakka.couchbase.SingleOperationResult
//  val document: JsonDocument = ???
//
//  val source: Source[JsonDocument, NotUsed] = Source.single(document)
//
//  val couchbaseWriteSettings: CouchbaseWriteSettings =
//    CouchbaseWriteSettings().withParallelism(2).withReplicateTo(ReplicateTo.NONE).withPersistTo(PersistTo.NONE)
//
//  val flow: Flow[JsonDocument, SingleOperationResult[JsonDocument], NotUsed] =
//    CouchbaseFlow2.upsertSingle(sessionSettings, couchbaseWriteSettings, bucketName)
//  source
//    .via(flow)
//    .map(response => {
//      response.result match {
//        case Failure(exception) => log.error(response.entity.id(), exception)
//        case Success(id) => log.info(s"Document with id $id successfully deleted.")
//      }
//      response.entity
//    })
//    .runWith(Sink.ignore)
//  //#init-SingleFlow
//}
