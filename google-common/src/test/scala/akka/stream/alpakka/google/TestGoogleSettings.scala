/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshaller}
import akka.stream.alpakka.google.auth.Credentials
import com.google

import scala.concurrent.{ExecutionContext, Future}

trait TestGoogleSettings {

  implicit def system: ClassicActorSystemProvider

  implicit val settings = GoogleSettings().copy(credentials = new Credentials {
    override def projectId: String = ???
    override def getToken()(implicit ec: ExecutionContext, settings: GoogleSettings): Future[OAuth2BearerToken] =
      Future.successful(OAuth2BearerToken("yyyy.c.an-access-token"))
    override def asGoogle(implicit ec: ExecutionContext, settings: GoogleSettings): google.auth.Credentials = ???
  })

  case class GoogleHttpException() extends Exception

  implicit val exceptionUnmarshaller: FromResponseUnmarshaller[Exception] = Unmarshaller(
    implicit ec =>
      (r: HttpResponse) => {
        r.discardEntityBytes().future().map(_ => GoogleHttpException())
      }
  )

}
