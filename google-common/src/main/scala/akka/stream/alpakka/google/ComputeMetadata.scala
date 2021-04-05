/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ErrorInfo, ExceptionWithErrorInfo, HttpRequest}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.google.auth.AccessToken

import java.time.Clock
import scala.concurrent.Future

@InternalApi
private[alpakka] object ComputeMetadata {

  private val `Metadata-Flavor` = RawHeader("Metadata-Flavor", "Google")

  private val metadataUrl = "http://metadata.google.internal/computeMetadata/v1"
  private def mkRequest(path: String) = HttpRequest(uri = s"$metadataUrl/$path").addHeader(`Metadata-Flavor`)

  private val projectIdRequest = mkRequest("project/project-id")
  private val zoneRequest = mkRequest("instance/zone")
  private val instanceIdRequest = mkRequest("instance/id")
  private val clusterNameRequest = mkRequest("instance/attributes/cluster-name")
  private val containerNameRequest = mkRequest("instance/attributes/container-name")
  private val namespaceIdRequest = mkRequest("instance/attributes/namespace-id")
  private val tokenRequest = mkRequest("instance/service-accounts/default/token")

  private def get(request: HttpRequest)(implicit mat: Materializer): Future[String] = {
    import mat.executionContext
    implicit val system = mat.system
    for {
      response <- Http().singleRequest(request)
      body <- Unmarshal(response.entity).to[String]
    } yield
      if (response.status.isSuccess)
        body
      else
        throw ComputeMetadataException(ErrorInfo(response.status.value, body))
  }

  def getProjectId(implicit mat: Materializer) = get(projectIdRequest)
  def getZone(implicit mat: Materializer) = get(zoneRequest)
  def getInstanceId(implicit mat: Materializer) = get(instanceIdRequest)
  def getClusterName(implicit mat: Materializer) = get(clusterNameRequest)
  def getContainerName(implicit mat: Materializer) = get(containerNameRequest)
  def getNamespaceId(implicit mat: Materializer) = get(namespaceIdRequest)

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

}

@InternalApi
private[alpakka] final case class ComputeMetadataException private (override val info: ErrorInfo)
    extends ExceptionWithErrorInfo(info)
