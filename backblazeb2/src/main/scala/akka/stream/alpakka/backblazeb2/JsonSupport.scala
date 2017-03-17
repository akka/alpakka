/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2
import akka.stream.alpakka.backblazeb2.Protocol.{ListFileVersionsResponse, _}
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import de.heikoseeberger.akkahttpcirce.CirceSupport._

object JsonSupport {
  implicit val accountAuthorizationTokenEncoder: Encoder[AccountAuthorizationToken] =
    Encoder.encodeString.contramap(_.value)

  implicit val accountAuthorizationTokenDecoder: Decoder[AccountAuthorizationToken] =
    Decoder.decodeString.map(AccountAuthorizationToken)

  implicit val uploadAuthorizationTokenEncoder: Encoder[UploadAuthorizationToken] =
    Encoder.encodeString.contramap(_.value)

  implicit val uploadAuthorizationTokenDecoder: Decoder[UploadAuthorizationToken] =
    Decoder.decodeString.map(UploadAuthorizationToken)

  implicit val accountIdEncoder: Encoder[AccountId] =
    Encoder.encodeString.contramap(_.value)

  implicit val accountIdDecoder: Decoder[AccountId] =
    Decoder.decodeString.map(AccountId)

  implicit val bucketIdEncoder: Encoder[BucketId] =
    Encoder.encodeString.contramap(_.value)

  implicit val bucketIdDecoder: Decoder[BucketId] =
    Decoder.decodeString.map(BucketId)

  implicit val fileIdEncoder: Encoder[FileId] =
    Encoder.encodeString.contramap(_.value)

  implicit val fileIdDecoder: Decoder[FileId] =
    Decoder.decodeString.map(FileId)

  implicit val fileNameEncoder: Encoder[FileName] =
    Encoder.encodeString.contramap(_.value)

  implicit val fileNameDecoder: Decoder[FileName] =
    Decoder.decodeString.map(FileName)

  implicit val sha1Encoder: Encoder[Sha1] =
    Encoder.encodeString.contramap(_.value)

  implicit val sha1Decoder: Decoder[Sha1] =
    Decoder.decodeString.map(Sha1)

  implicit val apiUrlEncoder: Encoder[ApiUrl] =
    Encoder.encodeString.contramap(_.value)

  implicit val apiUrlDecoder: Decoder[ApiUrl] =
    Decoder.decodeString.map(ApiUrl)

  implicit val uploadUrlEncoder: Encoder[UploadUrl] =
    Encoder.encodeString.contramap(_.value)

  implicit val uploadUrlDecoder: Decoder[UploadUrl] =
    Decoder.decodeString.map(UploadUrl)

  implicit val authorizeAccountResponseDecoder = Decoder[AuthorizeAccountResponse]
  implicit val authorizeAccountResponseEncoder = Encoder[AuthorizeAccountResponse]
  implicit val authorizeAccountResponseUnmarshaller = circeUnmarshaller[AuthorizeAccountResponse]

  implicit val getUploadUrlResponseDecoder = Decoder[GetUploadUrlResponse]
  implicit val getUploadUrlResponseEncoder = Encoder[GetUploadUrlResponse]
  implicit val getUploadUrlResponseUnmarshaller = circeUnmarshaller[GetUploadUrlResponse]

  implicit val uploadFileResponseDecoder = Decoder[UploadFileResponse]
  implicit val uploadFileResponseEncoder = Encoder[UploadFileResponse]
  implicit val getUploadFileResponseUnmarshaller = circeUnmarshaller[UploadFileResponse]

  implicit val listFileVersionsResponseDecoder = Decoder[ListFileVersionsResponse]
  implicit val listFileVersionsResponseUnmarshaller = circeUnmarshaller[ListFileVersionsResponse]

  implicit val fileVersionInfoDecoder = Decoder[FileVersionInfo]
  implicit val fileVersionInfoUnmarshaller = circeUnmarshaller[FileVersionInfo]

  implicit val b2ErrorResponseDecoder = Decoder[B2ErrorResponse]
  implicit val b2ErrorResponseEncoder = Encoder[B2ErrorResponse]
  implicit val b2ErrorResponseUnmarshaller = circeUnmarshaller[B2ErrorResponse]
}
