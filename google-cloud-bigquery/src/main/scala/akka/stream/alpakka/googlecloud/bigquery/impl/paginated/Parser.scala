/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.paginated

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.alpakka.googlecloud.bigquery.model.ResponseMetadataJsonProtocol.ResponseMetadata
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.{FanOutShape2, Graph, Materializer}

import scala.concurrent.Future

@InternalApi
private[paginated] object Parser {

  def apply[T, Json](
      implicit mat: Materializer,
      responseUnmarshaller: Unmarshaller[Json, ResponseMetadata],
      unmarshaller: Unmarshaller[Json, T]
  ): Graph[FanOutShape2[Json, T, PagingInfo], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    import mat.executionContext

    val parseMap = builder.add(Flow[Json].mapAsync(1)(Unmarshal(_).to[T]))
    val pageInfoProvider = builder.add(Flow[Json].mapAsync(1)(getPagingInfo[Json]))

    val broadcast = builder.add(Broadcast[Json](2, eagerCancel = true))

    broadcast.out(0) ~> parseMap
    broadcast.out(1) ~> pageInfoProvider

    new FanOutShape2(broadcast.in, parseMap.out, pageInfoProvider.out)
  }

  private def getPagingInfo[Json](json: Json)(
      implicit mat: Materializer,
      unmarshaller: Unmarshaller[Json, ResponseMetadata]
  ): Future[PagingInfo] = {
    import mat.executionContext

    Unmarshal(json).to[ResponseMetadata].fast.map { response =>
      val pageToken = response.pageToken orElse response.nextPageToken
      val jobInfo = for {
        jobReference <- response.jobReference
        jobComplete <- response.jobComplete
      } yield JobInfo(jobReference, jobComplete)

      PagingInfo(jobInfo, pageToken)
    }
  }
}
