/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.stream.alpakka.elasticsearch.ElasticsearchConnectionSettings

import scala.concurrent.Future

@InternalApi private[impl] object ElasticsearchApi {
  def executeRequest(
      request: HttpRequest,
      connectionSettings: ElasticsearchConnectionSettings
  )(implicit http: HttpExt): Future[HttpResponse] = {
    if (connectionSettings.hasCredentialsDefined) {
      http.singleRequest(
        request.addCredentials(BasicHttpCredentials(connectionSettings.username.get, connectionSettings.password.get))
      )
    } else {
      http.singleRequest(request,
                         connectionContext =
                           connectionSettings.connectionContext.getOrElse(http.defaultClientHttpsContext))
    }
  }
}
