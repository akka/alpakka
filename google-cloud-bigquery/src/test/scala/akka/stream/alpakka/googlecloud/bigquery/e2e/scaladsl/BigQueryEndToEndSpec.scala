/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e.scaladsl

import akka.actor.ActorSystem
import akka.{pattern, Done}
import akka.stream.alpakka.googlecloud.bigquery.HoverflySupport
import akka.stream.alpakka.googlecloud.bigquery.e2e.{A, B, C}
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.DoneState
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableReference
import akka.testkit.TestKit
import io.specto.hoverfly.junit.core.{HoverflyMode, SimulationSource}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import java.io.File
import scala.concurrent.Future
import scala.concurrent.duration._

class BigQueryEndToEndSpec
    extends TestKit(ActorSystem("BigQueryEndToEndSpec"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with HoverflySupport
    with EndToEndHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    system.settings.config.getString("alpakka.google.bigquery.test.e2e-mode") match {
      case "simulate" =>
        hoverfly.simulate(SimulationSource.url(getClass.getClassLoader.getResource("BigQueryEndToEndSpec.json")))
      case "capture" => hoverfly.resetMode(HoverflyMode.CAPTURE)
      case _ => throw new IllegalArgumentException
    }
  }

  override def afterAll() = {
    system.terminate()
    if (hoverfly.getMode == HoverflyMode.CAPTURE)
      hoverfly.exportSimulation(new File("hoverfly/BigQueryEndToEndSpec.json").toPath)
    super.afterAll()
  }

  implicit def scheduler = system.scheduler

  "BigQuery Scala DSL" should {

    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQuery
    import akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema.BigQuerySchemas._
    import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryJsonProtocol._
    import akka.stream.scaladsl.{Sink, Source}

    implicit val cFormat = bigQueryJsonFormat1(C)
    implicit val bFormat = bigQueryJsonFormat2(B)
    implicit val aFormat = bigQueryJsonFormat7(A)
    implicit val cSchema = bigQuerySchema1(C)
    implicit val bSchema = bigQuerySchema2(B)
    implicit val aSchema = bigQuerySchema7(A)

    "create dataset" in {
      BigQuery.createDataset(datasetId).map { dataset =>
        dataset.datasetReference.datasetId shouldEqual datasetId
      }
    }

    "list new dataset" in {
      BigQuery.datasets.runWith(Sink.seq).map { datasets =>
        datasets.map(_.datasetReference.datasetId) should contain(datasetId)
      }
    }

    "create table" in {
      BigQuery.createTable[A](datasetId, tableId).map { table =>
        table.tableReference should matchPattern {
          case TableReference(_, `datasetId`, `tableId`) =>
        }
      }
    }

    "list new table" in {
      BigQuery.tables(datasetId).runWith(Sink.seq).map { tables =>
        tables.map(_.tableReference.tableId) should contain(tableId)
      }
    }

    "insert rows via streaming insert" in {
      // TODO To test requires a project with billing enabled
      pending
    }

    "insert rows via load jobs" in {
      Source(rows)
        .via(BigQuery.insertAllAsync[A](datasetId, tableId))
        .runWith(Sink.seq)
        .flatMap {
          case Seq(job) =>
            pattern
              .retry(
                () => {
                  BigQuery.job(job.jobReference.flatMap(_.jobId).get).flatMap { job =>
                    if (job.status.map(_.state).contains(DoneState))
                      Future.successful(job)
                    else
                      Future.failed(new RuntimeException("Job not done."))
                  }
                },
                60,
                if (hoverfly.getMode == HoverflyMode.SIMULATE) 0.seconds else 1.second
              )
              .map { job =>
                job.status.flatMap(_.errorResult) shouldBe None
              }
        }
    }

    "retrieve rows" in {
      BigQuery.tableData[A](datasetId, tableId).runWith(Sink.seq).map { retrievedRows =>
        retrievedRows should contain theSameElementsAs rows
      }
    }

    "run query" in {
      val query = s"SELECT string, record, integer FROM $datasetId.$tableId WHERE boolean;"
      BigQuery.query[(String, B, Int)](query, useLegacySql = false).runWith(Sink.seq).map { retrievedRows =>
        retrievedRows should contain theSameElementsAs rows.filter(_.boolean).map(a => (a.string, a.record, a.integer))
      }
    }

    "delete table" in {
      BigQuery.deleteTable(datasetId, tableId).map { done =>
        done shouldBe Done
      }
    }

    "not list deleted table" in {
      BigQuery.tables(datasetId).runWith(Sink.seq).map { tables =>
        tables.map(_.tableReference.tableId) shouldNot contain(tableId)
      }
    }

    "delete dataset" in {
      BigQuery.deleteDataset(datasetId).map { done =>
        done shouldBe Done
      }
    }

    "not list deleted dataset" in {
      BigQuery.datasets.runWith(Sink.seq).map { datasets =>
        datasets.map(_.datasetReference.datasetId) shouldNot contain(datasetId)
      }
    }
  }

}
