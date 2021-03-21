/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{Marshal, ToEntityMarshaller}
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpRequest, RequestEntity}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller}
import akka.stream.alpakka.google.GoogleAttributes
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.{
  TableDataInsertAllRequest,
  TableDataInsertAllResponse,
  TableDataListResponse
}
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryEndpoints, BigQueryException, InsertAllRetryPolicy}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import java.util.{SplittableRandom, UUID}
import scala.collection.immutable.Seq
import scala.concurrent.Future

private[scaladsl] trait BigQueryTableData { this: BigQueryRest =>

  /**
   * Lists the content of a table in rows.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list BigQuery reference]]
   *
   * @param datasetId dataset ID of the table to list
   * @param tableId table ID of the table to list
   * @param startIndex start row index of the table
   * @param maxResults row limit of the table
   * @param selectedFields subset of fields to return, supports select into sub fields. Example: `selectedFields = Seq("a", "e.d.f")`
   * @tparam Out the data model of each row
   * @return a [[akka.stream.scaladsl.Source]] that emits an [[Out]] for each row in the table
   */
  def tableData[Out](datasetId: String,
                     tableId: String,
                     startIndex: Option[Long] = None,
                     maxResults: Option[Int] = None,
                     selectedFields: Seq[String] = Seq.empty)(
      implicit um: FromEntityUnmarshaller[TableDataListResponse[Out]]
  ): Source[Out, Future[TableDataListResponse[Out]]] =
    source { settings =>
      import BigQueryException._
      val uri = BigQueryEndpoints.tableData(settings.projectId, datasetId, tableId)
      val query = ("startIndex" -> startIndex) ?+:
        ("maxResults" -> maxResults) ?+:
        ("selectedFields" -> (if (selectedFields.isEmpty) None else Some(selectedFields.mkString(",")))) ?+:
        Query.Empty
      paginatedRequest[TableDataListResponse[Out]](HttpRequest(uri = uri.withQuery(query)))
    }.wireTapMat(Sink.head)(Keep.right).mapConcat(_.rows.fold[List[Out]](Nil)(_.toList))

  /**
   * Streams data into BigQuery one record at a time without needing to run a load job
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll BigQuery reference]]
   *
   * @param datasetId dataset id of the table to insert into
   * @param tableId table id of the table to insert into
   * @param retryPolicy [[akka.stream.alpakka.googlecloud.bigquery.InsertAllRetryPolicy]] determining whether to retry and deduplicate
   * @param templateSuffix if specified, treats the destination table as a base template, and inserts the rows into an instance table named "{destination}{templateSuffix}"
   * @tparam In the data model for each record
   * @return a [[akka.stream.scaladsl.Sink]] that inserts each batch of [[In]] into the table
   */
  def insertAll[In](
      datasetId: String,
      tableId: String,
      retryPolicy: InsertAllRetryPolicy,
      templateSuffix: Option[String] = None
  )(implicit m: ToEntityMarshaller[TableDataInsertAllRequest[In]]): Sink[Seq[In], NotUsed] = {
    val requests = Flow[Seq[In]].statefulMapConcat { () =>
      val randomGen = new SplittableRandom

      xs => {
        val rows = xs.map { x =>
          val insertId =
            if (retryPolicy.deduplicate)
              Some(randomUUID(randomGen).toString)
            else
              None
          TableDataJsonProtocol.Row(insertId, x)
        }

        TableDataInsertAllRequest(None, None, templateSuffix, rows) :: Nil
      }
    }

    val errorSink = Sink.foreach[TableDataInsertAllResponse] { response =>
      response.insertErrors
        .flatMap(_.headOption)
        .flatMap(_.errors)
        .flatMap(_.headOption)
        .foreach(error => throw BigQueryException(error))
    }

    requests.via(insertAll(tableId, datasetId, retryPolicy.retry)).to(errorSink)
  }

  /**
   * Streams data into BigQuery one record at a time without needing to run a load job.
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll BigQuery reference]]
   *
   * @param datasetId dataset ID of the table to insert into
   * @param tableId table ID of the table to insert into
   * @param retryFailedRequests whether to retry failed requests
   * @tparam In the data model for each record
   * @return a [[akka.stream.scaladsl.Flow]] that sends each [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.TableDataInsertAllRequest]] and emits a [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.TableDataInsertAllResponse]] for each
   */
  def insertAll[In](datasetId: String, tableId: String, retryFailedRequests: Boolean)(
      implicit m: ToEntityMarshaller[TableDataInsertAllRequest[In]]
  ): Flow[TableDataInsertAllRequest[In], TableDataInsertAllResponse, NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        import BigQueryException._
        import SprayJsonSupport._
        implicit val system = mat.system
        implicit val ec = ExecutionContexts.parasitic
        implicit val settings = GoogleAttributes.resolveSettings(mat, attr)

        val uri = BigQueryEndpoints.tableDataInsertAll(settings.projectId, datasetId, tableId)
        val request = HttpRequest(POST, uri)

        val um = {
          val um = implicitly[FromResponseUnmarshaller[TableDataInsertAllResponse]]
          if (retryFailedRequests) um else um.withoutRetries
        }

        val pool = {
          val authority = BigQueryEndpoints.endpoint.authority
          GoogleHttp().cachedHostConnectionPool[TableDataInsertAllResponse](authority.host.address, authority.port)(um)
        }

        Flow[TableDataInsertAllRequest[In]]
          .mapAsync(1)(Marshal(_).to[RequestEntity])
          .map(request.withEntity)
          .via(pool)
      }
      .mapMaterializedValue(_ => NotUsed)

  private def randomUUID(randomGen: SplittableRandom): UUID = {
    var msb = randomGen.nextLong()
    var lsb = randomGen.nextLong()
    msb &= 0xFFFFFFFFFFFF0FFFL // clear version
    msb |= 0x0000000000004000L // set to version 4
    lsb &= 0x3FFFFFFFFFFFFFFFL // clear variant
    lsb |= 0x8000000000000000L // set to IETF variant
    new UUID(msb, lsb)
  }
}
