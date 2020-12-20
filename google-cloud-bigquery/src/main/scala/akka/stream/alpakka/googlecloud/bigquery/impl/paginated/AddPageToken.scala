/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.paginated

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpEntity.Empty
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri.Query
import akka.stream.alpakka.googlecloud.bigquery.BigQueryException
import akka.stream.scaladsl.Flow

@InternalApi
private[paginated] object AddPageToken {

  def apply(): Flow[(HttpRequest, Option[PagingInfo]), HttpRequest, NotUsed] =
    Flow[(HttpRequest, Option[PagingInfo])].map {
      case (request, pagingInfo) =>
        pagingInfo.fold(request) {
          case PagingInfo(jobInfo, pageToken) =>
            val getRequest =
              if (request.method == GET)
                request
              else
                jobInfo
                  .flatMap(_.jobReference.jobId)
                  .map(addPath(convertRequestToGet(request), _))
                  .getOrElse {
                    throw BigQueryException(
                      "Cannot get the next page of a query request without a JobId in the response."
                    )
                  }

            if (jobInfo.forall(_.jobComplete))
              pageToken.fold(getRequest)(addPageToken(getRequest, _))
            else
              getRequest
        }
    }

  private def convertRequestToGet(req: HttpRequest): HttpRequest = req.withMethod(GET).withEntity(Empty)

  private def addPath(getReq: HttpRequest, path: String): HttpRequest =
    getReq.withUri(getReq.uri.withPath(getReq.uri.path ?/ path))

  private def addPageToken(request: HttpRequest, pageToken: String): HttpRequest = {
    val query = Query("pageToken" -> pageToken)
    request.withUri(request.uri.withQuery(query))
  }
}
