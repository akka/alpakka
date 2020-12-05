/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e
import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.bigquery.client.TableDataQueryJsonProtocol.Field
import akka.stream.alpakka.googlecloud.bigquery.client.TableListQueryJsonProtocol.{QueryTableModel, TableReference}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQueryCallbacks, GoogleBigQuerySource}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQueryJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.SprayJsonSupport._
import akka.stream.scaladsl.Sink
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class BigQueryEndToEndSpec extends BigQueryTableHelper with Matchers {
  override implicit val actorSystem: ActorSystem = ActorSystem("BigQueryEndToEndSpec")

  override def afterAll() = {
    super.afterAll
    actorSystem.terminate
  }

  "Google BigQuery" should {

    "list tables" in {
      val tables: Future[Seq[QueryTableModel]] =
        GoogleBigQuerySource.listTables(projectConfig).runWith(Sink.seq).map(_.flatten)
      await(tables) should contain(QueryTableModel(TableReference(tableName), "TABLE"))
    }

    "list fields" in {
      val fields: Future[Seq[Field]] =
        GoogleBigQuerySource.listFields(tableName, projectConfig).runWith(Sink.seq).map(_.flatten)
      await(fields).map(_.name).sorted shouldBe Seq("A1", "A2", "A3")
    }

    "select CSV style" in {
      val result =
        await(
          GoogleBigQuerySource
            .runQueryCsvStyle(s"SELECT * FROM $dataset.$tableName;", BigQueryCallbacks.ignore, projectConfig)
            .runWith(Sink.seq)
        )

      println(result)

      checkCsvStyleResultWithoutRowOrder(
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

    "select into case class" in {

      case class Row(A1: Option[String], A2: Option[Int], A3: Option[Boolean])
      implicit val rowFormatter = bigQueryJsonFormat3(Row)

      val result =
        await(
          GoogleBigQuerySource[Row]
            .runQuery(s"SELECT * FROM $dataset.$tableName;", BigQueryCallbacks.ignore, projectConfig)
            .runWith(Sink.seq)
        )

      checkCaseClassResultWithoutOrder(
        result,
        Seq(
          Row(Some("v1"), Some(1), Some(true)),
          Row(Some("v2"), Some(2), Some(false)),
          Row(Some("v3"), Some(3), Some(true)),
          Row(Some("v4"), Some(-4), Some(false)),
          Row(Some("v5"), None, Some(false)),
          Row(Some("v6"), Some(6), None),
          Row(Some("v7"), None, None)
        )
      )

    }

  }

  private def checkCsvStyleResultWithoutRowOrder(result: Seq[Seq[String]], expected: Seq[Seq[String]]): Unit = {
    result.size shouldEqual expected.size
    result.head.map(_.toUpperCase) shouldEqual expected.head.map(_.toUpperCase)
    result.foreach(expected contains _)
  }

  private def checkCaseClassResultWithoutOrder[T](result: Seq[T], expected: Seq[T]): Unit = {
    result.size shouldEqual expected.size
    result.foreach(expected contains _)
  }
}
