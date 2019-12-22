/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.bigquery.client.TableDataQueryJsonProtocol.Field
import akka.stream.alpakka.googlecloud.bigquery.client.TableListQueryJsonProtocol.{QueryTableModel, TableReference}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQueryCallbacks, GoogleBigQuerySource}
import akka.stream.scaladsl.Sink
import org.scalatest.Matchers

import scala.concurrent.Future

class BigQueryEndToEndSpec extends BigQueryTableHelper with Matchers {
  override implicit val actorSystem: ActorSystem = ActorSystem("BigQueryEndToEndSpec")
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def afterAll() = {
    super.afterAll
    actorSystem.terminate
  }

  "Google BigQuery" should {

    "list tables" in {
      assume(enableE2E, "BigQuery env-vars not configures")
      val tables: Future[Seq[QueryTableModel]] = GoogleBigQuerySource.listTables(connection)
      await(tables) should contain(QueryTableModel(TableReference(tableName), "TABLE"))
    }

    "list fields" in {
      assume(enableE2E, "BigQuery env-vars not configures")
      val fields: Future[Seq[Field]] = GoogleBigQuerySource.listFields(tableName, connection)
      await(fields).map(_.name).sorted shouldBe Seq("A1", "A2", "A3")
    }

    "select" in {
      assume(enableE2E, "BigQuery env-vars not configures")
      val result =
        await(
          GoogleBigQuerySource
            .runQueryCsvStyle(s"SELECT * FROM $dataset.$tableName;", BigQueryCallbacks.ignore, connection)
            .runWith(Sink.seq)
        )

      checkResultWithoutRowOrder(
        result,
        Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v2", "2", "0"),
          Seq("v3", "3", "1"),
          Seq("v4", "-4", "0"),
          Seq("v5", "null", "0"),
          Seq("v6", "6", "null"),
          Seq("v7", "null", "null")
        )
      )
    }

  }

  private def checkResultWithoutRowOrder(result: Seq[Seq[String]], expected: Seq[Seq[String]]): Unit = {
    result.size shouldEqual expected.size
    result.head.map(_.toUpperCase) shouldEqual expected.head.map(_.toUpperCase)
    result.foreach(expected contains _)
  }
}
