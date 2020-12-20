/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e
import akka.actor.ActorSystem
import akka.{pattern, Done}
import akka.stream.alpakka.googlecloud.bigquery.HoverflySupport
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.DoneState
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableReference
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQuery
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema.BigQuerySchemas._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.SprayJsonSupport._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import io.specto.hoverfly.junit.core.{HoverflyMode, SimulationSource}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import java.nio.file.Path
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random

class BigQueryEndToEndSpec
    extends TestKit(ActorSystem("BigQueryEndToEndSpec"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with HoverflySupport {

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
      hoverfly.exportSimulation(Path.of("hoverfly/BigQueryEndToEndSpec.json"))
    super.afterAll()
  }

  implicit override def executionContext = system.dispatcher
  implicit def scheduler = system.scheduler

  case class A(integer: Int, long: Long, float: Float, double: Double, string: String, boolean: Boolean, record: B)
  case class B(nullable: Option[String], repeated: Seq[C])
  case class C(numeric: BigDecimal)

  implicit val cFormat = bigQueryJsonFormat1(C)
  implicit val bFormat = bigQueryJsonFormat2(B)
  implicit val aFormat = bigQueryJsonFormat7(A)
  implicit val cSchema = bigQuerySchema1(C)
  implicit val bSchema = bigQuerySchema2(B)
  implicit val aSchema = bigQuerySchema7(A)

  val rng = new Random(1234567890)

  val datasetId = f"e2e_dataset_${rng.nextInt(1000)}%03d"
  val tableId = f"e2e_table_${rng.nextInt(1000)}%03d"

  def randomC(): C = C(BigDecimal(f"${rng.nextInt(100)}.${rng.nextInt(100)}%02d"))
  def randomB(): B = B(
    if (rng.nextBoolean()) Some(rng.nextString(rng.nextInt(64))) else None,
    Seq.fill(rng.nextInt(16))(randomC())
  )
  def randomA(): A = A(
    rng.nextInt(),
    rng.nextLong(),
    rng.nextFloat(),
    rng.nextDouble(),
    rng.nextString(rng.nextInt(64)),
    rng.nextBoolean(),
    randomB()
  )

  val rows = List.fill(10)(randomA())

  "BigQuery" should {

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
      Future.successful(assert(true))
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
                    if (job.status.map(_.state).get == DoneState)
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
      BigQuery[A].tableData(datasetId, tableId).runWith(Sink.seq).map { retrievedRows =>
        retrievedRows should contain theSameElementsAs rows
      }
    }

    "run query" in {
      val query = s"SELECT string, record, integer FROM $datasetId.$tableId WHERE boolean;"
      BigQuery[(String, B, Int)].query(query, useLegacySql = false).runWith(Sink.seq).map { retrievedRows =>
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
