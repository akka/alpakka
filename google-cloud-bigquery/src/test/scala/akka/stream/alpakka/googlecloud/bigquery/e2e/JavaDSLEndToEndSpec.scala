/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e

import akka.http.javadsl.marshallers.jackson.Jackson
import akka.stream.alpakka.googlecloud.bigquery.e2e.BigQueryEndToEndSpec.{A, B}
import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.Dataset
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.{DoneState, Job}
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{
  createTableFieldSchema,
  createTableSchema,
  BooleanType,
  FloatType,
  IntegerType,
  NullableMode,
  NumericType,
  RecordType,
  RepeatedMode,
  RequiredMode,
  StringType,
  Table,
  TableReference
}
import akka.{pattern, Done}
import com.fasterxml.jackson.databind.JsonNode
import io.specto.hoverfly.junit.core.HoverflyMode

import java.util
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

class JavaDSLEndToEndSpec extends BigQueryEndToEndSpec {

  "BigQuery JavaDSL" should {

    import akka.stream.alpakka.googlecloud.bigquery.javadsl.jackson.BigQueryMarshallers
    import akka.stream.alpakka.googlecloud.bigquery.javadsl.BigQuery
    import akka.stream.javadsl.{Sink, Source}

    val settings = BigQuery.getSettings(system)

    val schema = createTableSchema(
      createTableFieldSchema("integer", IntegerType, util.Optional.of(RequiredMode)),
      createTableFieldSchema("long", IntegerType, util.Optional.of(RequiredMode)),
      createTableFieldSchema("float", FloatType, util.Optional.of(RequiredMode)),
      createTableFieldSchema("double", FloatType, util.Optional.of(RequiredMode)),
      createTableFieldSchema("string", StringType, util.Optional.of(RequiredMode)),
      createTableFieldSchema("boolean", BooleanType, util.Optional.of(RequiredMode)),
      createTableFieldSchema(
        "record",
        RecordType,
        util.Optional.of(RequiredMode),
        createTableFieldSchema("nullable", StringType, util.Optional.of(NullableMode)),
        createTableFieldSchema(
          "repeated",
          RecordType,
          util.Optional.of(RepeatedMode),
          createTableFieldSchema("numeric", NumericType, util.Optional.of(RequiredMode))
        )
      )
    )

    "create dataset" in {
      BigQuery.createDataset(datasetId, system, settings).toScala.map { dataset =>
        dataset.datasetReference.datasetId shouldEqual datasetId
      }
    }

    "list new dataset" in {
      BigQuery
        .listDatasets(
          util.OptionalInt.empty(),
          util.Optional.empty(),
          util.Collections.emptyMap()
        )
        .runWith(Sink.seq[Dataset], system)
        .toScala
        .map { datasets =>
          datasets.asScala.map(_.datasetReference.datasetId) should contain(datasetId)
        }
    }

    "create table" in {
      BigQuery.createTable(datasetId, tableId, schema, system, settings).toScala.map { table =>
        table.tableReference should matchPattern {
          case TableReference(_, `datasetId`, `tableId`) =>
        }
      }
    }

    "list new table" in {
      BigQuery.listTables(datasetId, util.OptionalInt.empty()).runWith(Sink.seq[Table], system).toScala.map { tables =>
        tables.asScala.map(_.tableReference.tableId) should contain(tableId)
      }
    }

    "insert rows via streaming insert" in {
      // TODO To test requires a project with billing enabled
      pending
    }

    "insert rows via load jobs" in {
      Source
        .from(rows.asJava)
        .via(BigQuery.insertAllAsync[A](datasetId, tableId, Jackson.marshaller()))
        .runWith(Sink.seq[Job], system)
        .toScala
        .map(_.asScala.toList)
        .flatMap {
          case Seq(job) =>
            pattern
              .retry(
                () => {
                  BigQuery
                    .getJob(job.jobReference.flatMap(_.jobId).get, util.Optional.empty(), system, settings)
                    .toScala
                    .flatMap { job =>
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
      BigQuery
        .listTableData[A](
          datasetId,
          tableId,
          util.OptionalLong.empty(),
          util.OptionalInt.empty(),
          util.Collections.emptyList(),
          BigQueryMarshallers.tableDataListResponseUnmarshaller(classOf[A])
        )
        .runWith(Sink.seq[A], system)
        .toScala
        .map { retrievedRows =>
          retrievedRows.asScala should contain theSameElementsAs rows
        }
    }

    "run query" in {
      val query = s"SELECT string, record, integer FROM $datasetId.$tableId WHERE boolean;"
      BigQuery
        .query[JsonNode](query, false, false, BigQueryMarshallers.queryResponseUnmarshaller(classOf[JsonNode]))
        .runWith(Sink.seq[JsonNode], system)
        .toScala
        .map { retrievedRows =>
          retrievedRows.asScala.map { node =>
            (
              node.get("f").get(0).get("v").textValue(),
              new B(node.get("f").get(1).get("v")),
              node.get("f").get(2).get("v").textValue().toInt
            )
          } should contain theSameElementsAs rows
            .filter(_.boolean)
            .map(a => (a.string, a.record, a.integer))
        }
    }

    "delete table" in {
      BigQuery.deleteTable(datasetId, tableId, system, settings).toScala.map { done =>
        done shouldBe Done
      }
    }

    "not list deleted table" in {
      BigQuery.listTables(datasetId, util.OptionalInt.empty()).runWith(Sink.seq[Table], system).toScala.map { tables =>
        tables.asScala.map(_.tableReference.tableId) shouldNot contain(tableId)
      }
    }

    "delete dataset" in {
      BigQuery.deleteDataset(datasetId, false, system, settings).toScala.map { done =>
        done shouldBe Done
      }
    }

    "not list deleted dataset" in {
      BigQuery
        .listDatasets(
          util.OptionalInt.empty(),
          util.Optional.empty(),
          util.Collections.emptyMap()
        )
        .runWith(Sink.seq[Dataset], system)
        .toScala
        .map { datasets =>
          datasets.asScala.map(_.datasetReference.datasetId) shouldNot contain(datasetId)
        }
    }
  }

}
