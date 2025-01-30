/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.scaladsl

import akka.http.javadsl.{model => jm}
import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}
import akka.http.scaladsl.model.{ContentType, ErrorInfo, IllegalHeaderException}
import akka.stream.alpakka.google.javadsl.XUploadContentType

import scala.util.{Failure, Success, Try}

/**
 * Models the `X-Upload-Content-Type` header for resumable uploads.
 */
object `X-Upload-Content-Type` extends ModeledCustomHeaderCompanion[`X-Upload-Content-Type`] {
  override def name: String = "X-Upload-Content-Type"
  override def parse(value: String): Try[`X-Upload-Content-Type`] =
    ContentType
      .parse(value)
      .fold(
        errorInfos => Failure(new IllegalHeaderException(errorInfos.headOption.getOrElse(ErrorInfo()))),
        contentType => Success(`X-Upload-Content-Type`(contentType))
      )
}

final case class `X-Upload-Content-Type` private[akka] (contentType: ContentType)
    extends ModeledCustomHeader[`X-Upload-Content-Type`]
    with XUploadContentType {
  override def value(): String = contentType.toString()
  override def renderInRequests(): Boolean = true
  override def renderInResponses(): Boolean = false
  override def companion = `X-Upload-Content-Type`

  /**
   * Java API
   */
  override def getContentType: jm.ContentType = ???
}
