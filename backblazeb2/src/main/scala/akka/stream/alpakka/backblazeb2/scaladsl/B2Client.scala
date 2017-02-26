/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.stream.alpakka.backblazeb2.B2Encoder.encodeBase64
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, ResponseEntity}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.backblazeb2.B2Exception
import akka.stream.alpakka.backblazeb2.Protocol.AuthorizeAccountResponse
import akka.stream.alpakka.backblazeb2.JsonSupport._
import scala.concurrent.{ExecutionContext, Future}

class B2Client(implicit system: ActorSystem, materializer: Materializer) {
  private def entityForSuccess(resp: HttpResponse)(implicit ctx: ExecutionContext): Future[ResponseEntity] =
    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        Future.successful(entity)

      case HttpResponse(status, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap(err => Future.failed(new B2Exception(s"HTTP error $status - $err")))
    }

  /**
   * https://www.backblaze.com/b2/docs/b2_authorize_account.html
   */
  def authorizeAccount(accountId: String, applicationKey: String): Future[AuthorizeAccountResponse] = {
    implicit val executionContext = materializer.executionContext
    val encodedCredentials = encodeBase64(s"$accountId:$applicationKey")
    val authorization = s"Basic $encodedCredentials"
    val request = HttpRequest(uri = "https://api.backblazeb2.com/b2api/v1/b2_authorize_account")
      .withHeaders(RawHeader("Authorization", authorization))

    for {
      response <- Http().singleRequest(request)
      entity <- entityForSuccess(response)
      result <- parseAuthorizeAccountResponseToToken(entity)
    } yield result
  }

  private def parseAuthorizeAccountResponseToToken(
    entity: ResponseEntity
  )(implicit exeutionContext: ExecutionContext): Future[AuthorizeAccountResponse] = {
    import de.heikoseeberger.akkahttpcirce.CirceSupport._
    Unmarshal(entity).to[AuthorizeAccountResponse]
  }

  // https://www.backblaze.com/b2/docs/b2_download_file_by_name.html
  // https://www.backblaze.com/b2/docs/b2_get_upload_url.html
  // https://www.backblaze.com/b2/docs/b2_upload_file.html
}
