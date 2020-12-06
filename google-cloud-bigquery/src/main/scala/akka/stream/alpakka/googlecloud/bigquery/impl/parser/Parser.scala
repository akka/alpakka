/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.parser

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.unmarshalling.{FromByteStringUnmarshaller, Unmarshal, Unmarshaller}
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.alpakka.googlecloud.bigquery.client.ResponseJsonProtocol
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.{FanOutShape2, Graph, Materializer}

import scala.concurrent.{ExecutionContext, Future}

@InternalApi
private[bigquery] object Parser {
  final case class PagingInfo(pageToken: Option[String], jobId: Option[String])

  def apply[J, T](
      implicit materializer: Materializer,
      ec: ExecutionContext,
      jsonUnmarshaller: FromByteStringUnmarshaller[J],
      responseUnmarshaller: Unmarshaller[J, ResponseJsonProtocol.Response],
      unmarshaller: Unmarshaller[J, T]
  ): Graph[FanOutShape2[HttpResponse, T, (Boolean, PagingInfo)], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val bodyJsonParse = builder.add(Flow[HttpResponse].mapAsync(1)(parseHttpResponse(_)))

    val parseMap = builder.add(Flow[J].mapAsync(1)(Unmarshal(_).to[T]))
    val pageInfoProvider = builder.add(Flow[J].mapAsync(1)(getPageInfo[J]))

    val broadcast1 = builder.add(Broadcast[J](2, eagerCancel = true))

    bodyJsonParse ~> broadcast1

    broadcast1.out(0) ~> parseMap
    broadcast1.out(1) ~> pageInfoProvider

    new FanOutShape2(bodyJsonParse.in, parseMap.out, pageInfoProvider.out)
  }

  private def parseHttpResponse[J](
      response: HttpResponse
  )(implicit materializer: Materializer,
    ec: ExecutionContext,
    jsonUnmarshaller: FromByteStringUnmarshaller[J]): Future[J] = {
    implicit val unmarshaller =
      Unmarshaller.byteStringUnmarshaller.forContentTypes(`application/json`).andThen(jsonUnmarshaller)
    Unmarshal(response.entity).to[J]
  }

  private def getPageInfo[J](json: J)(
      implicit materializer: Materializer,
      ec: ExecutionContext,
      unmarshaller: Unmarshaller[J, ResponseJsonProtocol.Response]
  ): Future[(Boolean, PagingInfo)] = {

    Unmarshal(json).to[ResponseJsonProtocol.Response].fast.map { response =>
      val pageToken = response.pageToken orElse response.nextPageToken
      val jobId = response.jobReference.flatMap(_.jobId)
      val retry = !response.jobComplete.getOrElse(true)

      (retry, PagingInfo(pageToken, jobId))
    }
  }
}
