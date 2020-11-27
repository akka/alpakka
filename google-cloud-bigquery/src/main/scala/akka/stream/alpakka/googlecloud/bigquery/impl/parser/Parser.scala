/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.parser

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.{FromByteStringUnmarshaller, Unmarshal, Unmarshaller}
import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.alpakka.googlecloud.bigquery.BigQueryJsonProtocol
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Zip}
import akka.stream.{FanOutShape2, FlowShape, Graph, Materializer}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@InternalApi
private[bigquery] object Parser {
  final case class PagingInfo(pageToken: Option[String], jobId: Option[String])

  def apply[J, T](
      implicit materializer: Materializer,
      ec: ExecutionContext,
      jsonUnmarshaller: FromByteStringUnmarshaller[J],
      responseUnmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response],
      unmarshaller: Unmarshaller[J, T]
  ): Graph[FanOutShape2[HttpResponse, T, (Boolean, PagingInfo)], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val bodyJsonParse: FlowShape[HttpResponse, J] = builder.add(Flow[HttpResponse].mapAsync(1)(parseHttpBody(_)))

    val parseMap = builder.add(Flow[J].mapAsync(1)(parseJson[J, T]))
    val pageInfoProvider = builder.add(Flow[J].mapAsync(1)(getPageInfo[J]))

    val broadcast1 = builder.add(Broadcast[J](2, eagerCancel = true))
    val broadcast2 = builder.add(Broadcast[Try[T]](2, eagerCancel = true))

    val filterNone = builder.add(Flow[Try[T]].mapConcat {
      case Success(value) => List(value)
      case Failure(e) => List()
    })

    val mapOptionToBool = builder.add(Flow[Try[T]].map(_.isFailure))

    val zip = builder.add(Zip[Boolean, PagingInfo]())

    bodyJsonParse ~> broadcast1

    broadcast1.out(0) ~> parseMap
    broadcast1.out(1) ~> pageInfoProvider

    parseMap ~> broadcast2

    broadcast2.out(0) ~> filterNone
    broadcast2.out(1) ~> mapOptionToBool

    mapOptionToBool ~> zip.in0
    pageInfoProvider ~> zip.in1

    new FanOutShape2(bodyJsonParse.in, filterNone.out, zip.out)
  }

  private def parseHttpBody[J](
      response: HttpResponse
  )(implicit materializer: Materializer,
    ec: ExecutionContext,
    jsonUnmarshaller: FromByteStringUnmarshaller[J]): Future[J] = {
    implicit val unmarshaller = Unmarshaller.byteStringUnmarshaller
      .map { bs =>
        if (bs.isEmpty)
          ByteString("{}")
        else
          bs
      }
      .andThen(jsonUnmarshaller)
    Unmarshal(response.entity).to[J]
  }

  private def parseJson[J, T](json: J)(
      implicit materializer: Materializer,
      ec: ExecutionContext,
      unmarshaller: Unmarshaller[J, T]
  ): Future[Try[T]] = {
    Unmarshal(json)
      .to[T]
      .fast
      .transformWith(FastFuture.successful[Try[T]])
  }

  private def getPageInfo[J](json: J)(
      implicit materializer: Materializer,
      ec: ExecutionContext,
      unmarshaller: Unmarshaller[J, BigQueryJsonProtocol.Response]
  ): Future[PagingInfo] = {

    Unmarshal(json).to[BigQueryJsonProtocol.Response].fast.map { response =>
      val pageToken = response.pageToken orElse response.nextPageToken
      val jobId = response.jobReference.flatMap(_.jobId)

      PagingInfo(pageToken, jobId)
    }

  }
}
