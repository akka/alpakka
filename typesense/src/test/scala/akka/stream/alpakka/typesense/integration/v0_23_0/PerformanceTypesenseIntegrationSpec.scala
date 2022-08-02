/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration.v0_23_0

import akka.Done
import akka.stream.alpakka.typesense.integration.TypesenseIntegrationSpec
import akka.stream.alpakka.typesense.integration.v0_23_0.PerformanceTypesenseIntegrationSpec.Company
import akka.stream.alpakka.typesense.scaladsl.Typesense
import akka.stream.alpakka.typesense._
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.{Assertion, Ignore}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

@Ignore
/** Simple performance test - index documents one by one using workers (in parallel) and check if all are indexed correctly.*/
class PerformanceTypesenseIntegrationSpec extends TypesenseIntegrationSpec("0.23.0") {
  import system.dispatcher
  val collectionName = "companies"
  def createCollection(): CollectionResponse = {
    val schema = CollectionSchema(
      "companies",
      Seq(Field("id", FieldType.String),
          Field("name", FieldType.String),
          Field("budget", FieldType.Int32),
          Field("evaluation", FieldType.Float))
    )
    runWithFlowTypesenseResult(schema, Typesense.createCollectionFlow(settings))
  }

  def indexDocuments(totalWorkers: Int, totalDocuments: Int): Unit = {
    val indexFlow = Typesense.indexDocumentFlow[Company](settings)

    def runWorker(workerNr: Int): Future[Unit] = {

      val indexSource = Source(1 to totalDocuments)
        .filter(n => n % totalWorkers == workerNr)
        .map(n => Company(n.toString, s"Company-$n", n * 1000, n / 10.0))
        .map(c => IndexDocument(collectionName, c))

      val startTime = System.nanoTime()
      val logerName = s"worker-$workerNr"
      println(s"[$logerName] Start indexing")

      indexSource
        .via(indexFlow)
        .flatMapConcat {
          case _: SuccessTypesenseResult[Done] => Source.empty
          case result: FailureTypesenseResult => Source.single(result.reason)
        }
        .log(logerName, identity)
        .run()
        .map { _ =>
          val endTime = System.nanoTime()
          val time = (endTime - startTime) / 1e9
          println(s"[$logerName] Indexing finished: $time s")
        }
    }

    val res: Future[Done] = Source(0 until totalWorkers).mapAsync(parallelism = totalWorkers)(runWorker).run()

    Await.result(res, 60.minutes)

    println("All indexing finished")
  }

  def checkCollection(totalDocuments: Int): Assertion = {
    val collection: CollectionResponse = Await.result(
      Source
        .single(RetrieveCollection("companies"))
        .via(Typesense.retrieveCollectionFlow(settings))
        .map {
          case result: SuccessTypesenseResult[CollectionResponse] => result.value
          case result: FailureTypesenseResult => fail(s"Cannot fetch collection: ${result.reason}")
        }
        .toMat(Sink.head)(Keep.right)
        .run(),
      10.seconds
    )

    println(s"Collection fetched: $collection")
    collection.numDocuments shouldBe totalDocuments
  }

  def fetchEachDocument(totalDocuments: Int): Unit = {
    val startTime = System.nanoTime()
    println("Start fetching")
    val fetchResult = Await.result(
      Source(1 to totalDocuments)
        .map(n => RetrieveDocument(collectionName, n.toString))
        .via(Typesense.retrieveDocumentFlow[Company](settings))
        .runForeach { res =>
          val c: Company = res.asInstanceOf[SuccessTypesenseResult[Company]].value
          assert(c.name == s"Company-${c.id}")
        },
      60.minutes
    )

    assert(fetchResult == Done)

    val endTime = System.nanoTime()
    val time = (endTime - startTime) / 1e9
    println(s"Fetching finished: $time s")
  }

  describe("Documents") {
    it("should be indexed and retrieved") {
      val totalWorkers = 5
      val totalDocuments = 100_000

      createCollection()
      indexDocuments(totalWorkers, totalDocuments)
      checkCollection(totalDocuments)
      fetchEachDocument(totalDocuments)
    }
  }
}

object PerformanceTypesenseIntegrationSpec {
  import spray.json._
  import DefaultJsonProtocol._
  final case class Company(id: String, name: String, budget: Int, evaluation: Double = 4.3)
  implicit val companyFormat: RootJsonFormat[Company] = jsonFormat4(Company)
}
