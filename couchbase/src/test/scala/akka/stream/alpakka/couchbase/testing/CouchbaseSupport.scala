/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.testing

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.scaladsl._
import akka.stream.alpakka.couchbase.{CouchbaseSessionSettings, CouchbaseWriteSettings}
import akka.stream.scaladsl.{Sink, Source}
import com.couchbase.client.deps.io.netty.buffer.Unpooled
import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.auth.PasswordAuthenticator
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{BinaryDocument, JsonDocument, RawJsonDocument, StringDocument}
import com.couchbase.client.java.{CouchbaseCluster, ReplicateTo}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import scala.collection.immutable.Seq
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class TestObject(id: String, value: String)

trait CouchbaseSupport {

  private val log = LoggerFactory.getLogger(classOf[CouchbaseSupport])

  //#init-actor-system
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  //#init-actor-system

  val sampleData = TestObject("First", "First")

  val sampleSequence: Seq[TestObject] = sampleData +: Seq[TestObject](TestObject("Second", "Second"),
                                                                      TestObject("Third", "Third"),
                                                                      TestObject("Fourth", "Fourth"))

  val sampleJavaList: java.util.List[TestObject] = sampleSequence.asJava

  val cluster: CouchbaseCluster = CouchbaseCluster.create("localhost")

  cluster.authenticate(new PasswordAuthenticator("Administrator", "password"))

  val sessionSettings = CouchbaseSessionSettings(actorSystem)
  val writeSettings: CouchbaseWriteSettings = CouchbaseWriteSettings().withReplicateTo(ReplicateTo.NONE)
  val bucketName = "akka"
  val queryBucketName = "akkaquery"

  var session: CouchbaseSession = _

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
    Await.result(createIndex, 10.seconds)
    Thread.sleep(4000)

    session = Await.result(CouchbaseSession(sessionSettings, bucketName), 2.seconds)
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

  def upsertSampleData(): Unit = {
    val bulkUpsertResult: Future[Done] = Source(sampleSequence)
      .map(toJsonDocument)
      .via(CouchbaseFlow.upsert(sessionSettings, CouchbaseWriteSettings.inMemory, queryBucketName))
      .runWith(Sink.ignore)
    Await.result(bulkUpsertResult, 5.seconds)
    //all queries are Eventual Consistent, se we need to wait for index refresh!!
    Thread.sleep(2000)
  }

  def cleanAllInBucket(ids: Seq[String], bucketName: String): Unit = {
    val result: Future[Done] =
      Source(ids)
        .via(CouchbaseFlow.deleteWithResult(sessionSettings, CouchbaseWriteSettings.inMemory, bucketName))
        .runWith(Sink.ignore)
    Await.result(result, 5.seconds)
    ()
  }

  def afterAll(): Unit =
    actorSystem.terminate()
}

final class CouchbaseSupportClass extends CouchbaseSupport
