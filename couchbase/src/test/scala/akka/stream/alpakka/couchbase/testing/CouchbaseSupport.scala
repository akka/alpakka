/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.testing

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.couchbase.{CouchbaseDocument, CouchbaseSessionSettings}
import akka.stream.alpakka.couchbase.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

case class TestObject(id: String, value: String)

trait CouchbaseSupport {

  private val log = LoggerFactory.getLogger(classOf[CouchbaseSupport])

  //#init-actor-system
  implicit val actorSystem: ActorSystem = ActorSystem()
  //#init-actor-system

  val sampleData = new CouchbaseDocument("First", "First")

  val sampleSequence: Seq[CouchbaseDocument[String]] = sampleData +: Seq[CouchbaseDocument[String]](
      new CouchbaseDocument("Second", "Second"),
      new CouchbaseDocument("Third", "Third"),
      new CouchbaseDocument("Fourth", "Fourth")
    )

  val sampleJavaList: java.util.List[CouchbaseDocument[String]] = sampleSequence.asJava

  val sessionSettings = CouchbaseSessionSettings(actorSystem)
  val bucketName = "akka"
  val scopeName = "alpakka"
  val collectionName = "alpakka-collection"

  var session: CouchbaseSession = _

  def beforeAll(): Unit = {
    session = Await.result(CouchbaseSession(sessionSettings, bucketName), 10.seconds)
    log.info("Done Creating CB Server")
  }

  def upsertSampleData(bucketName: String, scopeName: String, collectionName: String): Unit = {
    val bulkUpsertResult: Future[Done] = Source(sampleSequence)
      .via(CouchbaseFlow.upsert(sessionSettings, bucketName, scopeName, collectionName))
      .runWith(Sink.ignore)
    Await.result(bulkUpsertResult, 10.seconds)
    //all queries are Eventual Consistent, se we need to wait for index refresh!!
    Thread.sleep(5000)
  }

  def cleanAllInCollection(bucketName: String, scopeName: String, collectionName: String): Unit =
    cleanAllInCollection(sampleSequence.map(_.getId), bucketName, scopeName, collectionName)

  def cleanAllInCollection(ids: Seq[String], bucketName: String, scopeName: String, collectionName: String): Unit = {
    val result: Future[Done] =
      Source(ids)
        .via(CouchbaseFlow.deleteWithResult(sessionSettings, bucketName, scopeName, collectionName))
        .runWith(Sink.ignore)
    Await.result(result, 5.seconds)
    ()
  }

  def afterAll(): Unit =
    actorSystem.terminate()
}

final class CouchbaseSupportClass extends CouchbaseSupport
