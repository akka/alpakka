/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.pagetoken

import java.net.URLEncoder

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.scaladsl.Flow

@InternalApi
private[impl] object AddPageToken {
  def apply(): Flow[(HttpRequest, (Boolean, PagingInfo)), HttpRequest, NotUsed] =
    Flow[(HttpRequest, (Boolean, PagingInfo))].map { case (request, (retry, pagingInfo)) =>
      val req = if (!retry) {
        addPageToken(request, pagingInfo)
      } else {
        request
      }

      pagingInfo.jobId match {
        case Some(id) =>
          val getReq = convertRequestToGet(req)
          addPath(getReq, id)
        case None => req
      }
    }

  private def addPath(getReq: HttpRequest, path: String) =
    getReq.withUri(getReq.uri.withPath(getReq.uri.path ?/ path))

  private def convertRequestToGet(req: HttpRequest) = req.withMethod(HttpMethods.GET).withEntity(HttpEntity.Empty)

  private def addPageToken(request: HttpRequest, pagingInfo: PagingInfo) = request.withUri(
    request.uri
      .withQuery(Uri.Query(pagingInfo.pageToken.map(token => s"pageToken=${URLEncoder.encode(token, "utf-8")}")))
  )
}
