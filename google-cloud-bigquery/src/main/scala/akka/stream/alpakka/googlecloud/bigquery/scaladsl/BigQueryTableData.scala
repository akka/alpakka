/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.NotUsed
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{Marshal, ToEntityMarshaller}
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpRequest, RequestEntity}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.{
  TableDataInsertAllRequest,
  TableDataInsertAllResponse,
  TableDataListResponse
}
import akka.stream.alpakka.googlecloud.bigquery.{
  BigQueryAttributes,
  BigQueryEndpoints,
  BigQueryException,
  InsertAllRetryPolicy
}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import java.util.{SplittableRandom, UUID}
import scala.collection.immutable.Seq
import scala.concurrent.Future

private[scaladsl] trait BigQueryTableData { this: BigQueryRest =>

  def tableData[Out](datasetId: String,
                     tableId: String,
                     startIndex: Option[Long] = None,
                     maxResults: Option[Int] = None,
                     selectedFields: Seq[String] = Seq.empty)(
      implicit um: FromEntityUnmarshaller[TableDataListResponse[Out]]
  ): Source[Out, Future[TableDataListResponse[Out]]] =
    source { settings =>
      val uri = BigQueryEndpoints.tableData(settings.projectId, datasetId, tableId)
      val query = Query.Empty :+?
        "startIndex" -> startIndex :+?
        "maxResults" -> maxResults :+?
        "selectedFields" -> (if (selectedFields.isEmpty) None else Some(selectedFields.mkString(",")))
      paginatedRequest[TableDataListResponse[Out]](HttpRequest(GET, uri.withQuery(query)))
    }.wireTapMat(Sink.head)(Keep.right).mapConcat(_.rows.fold[List[Out]](Nil)(_.toList))

  def insertAll[In](
      datasetId: String,
      tableId: String,
      retryPolicy: InsertAllRetryPolicy,
      templateSuffix: Option[String] = None
  )(implicit marshaller: ToEntityMarshaller[TableDataInsertAllRequest[In]]): Sink[Seq[In], NotUsed] = {
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

  def insertAll[In](datasetId: String, tableId: String, retryFailedRequests: Boolean)(
      implicit marshaller: ToEntityMarshaller[TableDataInsertAllRequest[In]]
  ): Flow[TableDataInsertAllRequest[In], TableDataInsertAllResponse, NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        import BigQueryException._
        import SprayJsonSupport._
        import mat.executionContext
        implicit val system = mat.system
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)

        val uri = BigQueryEndpoints.tableDataInsertAll(settings.projectId, datasetId, tableId)
        val request = HttpRequest(POST, uri)

        val http = BigQueryHttp()
        val requestWithOAuth =
          if (retryFailedRequests)
            http.retryRequestWithOAuth(_)
          else
            http.singleRequestWithOAuthOrFail(_)

        Flow[TableDataInsertAllRequest[In]]
          .mapAsync(1)(Marshal(_).to[RequestEntity])
          .map(request.withEntity)
          .mapAsync(1)(requestWithOAuth)
          .mapAsync(1)(Unmarshal(_).to[TableDataInsertAllResponse])
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
