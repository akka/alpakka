/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshaller}
import akka.stream.alpakka.google.implicits._

final case class GoogleHttpException() extends Exception

object GoogleHttpException {
  implicit val exceptionUnmarshaller: FromResponseUnmarshaller[Throwable] = Unmarshaller.withMaterializer {
    implicit ec => implicit mat => (r: HttpResponse) =>
      r.discardEntityBytes().future().map(_ => GoogleHttpException(): Throwable)
  }.withDefaultRetry
}
