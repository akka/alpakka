/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl.pagetoken

import java.net.URLEncoder

import akka.NotUsed
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import akka.stream.alpakka.google.cloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.scaladsl.Flow

object AddPageToken {
  def apply(): Flow[(HttpRequest, PagingInfo), HttpRequest, NotUsed] =
    Flow[(HttpRequest, PagingInfo)].map {
      case (request, pagingInfo) =>
        val req = addPageToken(request, pagingInfo)

        pagingInfo.jobId match {
          case Some(id) =>
            val getReq = convertRequestToGet(req)
            addPath(getReq, id)
          case None => req
        }
    }

  private def addPath(getReq: HttpRequest, path: String) =
    getReq.copy(uri = getReq.uri.withPath(getReq.uri.path ?/ path))

  private def convertRequestToGet(req: HttpRequest) = req.copy(method = HttpMethods.GET, entity = HttpEntity.Empty)

  private def addPageToken(request: HttpRequest, pagingInfo: PagingInfo) = request.copy(
    uri = request.uri
      .withQuery(Uri.Query(pagingInfo.pageToken.map(token => s"pageToken=${URLEncoder.encode(token, "utf-8")}")))
  )
}
