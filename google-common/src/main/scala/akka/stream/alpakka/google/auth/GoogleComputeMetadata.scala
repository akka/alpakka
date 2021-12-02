/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer

import java.time.Clock
import scala.concurrent.Future

@InternalApi
private[auth] object GoogleComputeMetadata {

  private val metadataUrl = "http://metadata.google.internal/computeMetadata/v1"
  private val tokenUrl = s"$metadataUrl/instance/service-accounts/default/token"
  private val projectIdUrl = s"$metadataUrl/project/project-id"
  private val `Metadata-Flavor` = RawHeader("Metadata-Flavor", "Google")

  private val tokenRequest = HttpRequest(GET, tokenUrl).addHeader(`Metadata-Flavor`)
  private val projectIdRequest = HttpRequest(GET, projectIdUrl).addHeader(`Metadata-Flavor`)

  def getAccessToken()(
      implicit mat: Materializer,
      clock: Clock
  ): Future[AccessToken] = {
    import SprayJsonSupport._
    import mat.executionContext
    implicit val system = mat.system
    for {
      response <- Http().singleRequest(tokenRequest)
      token <- Unmarshal(response.entity).to[AccessToken]
    } yield token
  }

  def getProjectId()(
      implicit mat: Materializer
  ): Future[String] = {
    import mat.executionContext
    implicit val system = mat.system
    for {
      response <- Http().singleRequest(projectIdRequest)
      projectId <- Unmarshal(response.entity).to[String]
    } yield projectId
  }
}
