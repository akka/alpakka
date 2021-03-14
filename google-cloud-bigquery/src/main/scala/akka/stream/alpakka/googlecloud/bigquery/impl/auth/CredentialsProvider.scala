/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.auth

import akka.annotation.InternalApi
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings

import scala.concurrent.{ExecutionContext, Future}

@InternalApi
private[bigquery] trait CredentialsProvider {
  def projectId: String
  def getToken()(implicit ec: ExecutionContext, settings: BigQuerySettings): Future[OAuth2BearerToken]
}
