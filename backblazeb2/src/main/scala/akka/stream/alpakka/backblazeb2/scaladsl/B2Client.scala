/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentType
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.util.ByteString
import cats.data.EitherT
import cats.syntax.either._
import scala.concurrent.{Future, Promise}

/**
 * Warning - Not thread safe.
 */
class B2Client(
    accountCredentials: B2AccountCredentials,
    bucketId: BucketId,
    hostAndPort: String = B2API.DefaultHostAndPort)(implicit system: ActorSystem, materializer: ActorMaterializer) {
  implicit val executionContext = materializer.executionContext
  private val api = new B2API(hostAndPort)

  private var authorizeAccountPromise: Promise[AuthorizeAccountResponse] = Promise()
  private var getUploadUrlPromise: Promise[GetUploadUrlResponse] = Promise()

  /**
   * Return the saved AuthorizeAccountResponse if it exists or obtain a new one if it doesn't
   */
  private def obtainAuthorizeAccountResponse(): B2Response[AuthorizeAccountResponse] =
    returnOrObtain(authorizeAccountPromise, callAuthorizeAccount)

  private def tryAgainIfExpired[T](x: B2Response[T])(fallbackIfExpired: => B2Response[T]): Future[Either[B2Error, T]] =
    x flatMap {
      case Left(error) if error.isExpiredToken =>
        fallbackIfExpired

      case Left(error) =>
        Future.successful(error.asLeft)

      case success: Right[B2Error, T] =>
        Future.successful(success)
    }

  /**
   * Return the saved GetUploadUrlResponse if it exists or obtain a new one if it doesn't
   */
  private def obtainGetUploadUrlResponse(): B2Response[GetUploadUrlResponse] =
    returnOrObtain(getUploadUrlPromise, callGetUploadUrl)

  private def callAuthorizeAccount(): B2Response[AuthorizeAccountResponse] =
    api.authorizeAccount(accountCredentials)

  private def callGetUploadUrl(): B2Response[GetUploadUrlResponse] = {
    import cats.implicits._
    val eitherT = for {
      authorizeAccountResponse <- EitherT(obtainAuthorizeAccountResponse())
      response <- EitherT(api.getUploadUrl(authorizeAccountResponse, bucketId))
    } yield response

    tryAgainIfExpired(eitherT.value) {
      authorizeAccountPromise = Promise()
      callGetUploadUrl()
    }
  }

  private def returnOrObtain[T](promise: Promise[T], f: () => B2Response[T]): B2Response[T] =
    if (promise.isCompleted) {
      promise.future.map(x => x.asRight[B2Error])
    } else {
      val result = f()

      result map { either => // saving if successful
        either map { x =>
          promise.success(x)
        }
      }

      result
    }

  def upload(fileName: FileName,
             data: ByteString,
             contentType: ContentType = DefaultContentType): B2Response[UploadFileResponse] = {
    import cats.implicits._
    val result = for {
      getUploadUrlResponse <- EitherT(obtainGetUploadUrlResponse())
      upload <- EitherT(api.uploadFile(getUploadUrlResponse, fileName, data, contentType))
    } yield upload

    tryAgainIfExpired(result.value) {
      getUploadUrlPromise = Promise()
      upload(fileName, data, contentType)
    }
  }

  def downloadById(fileId: FileId): B2Response[DownloadFileResponse] = {
    import cats.implicits._
    val result = for {
      authorizeAccountResponse <- EitherT(obtainAuthorizeAccountResponse())
      download <- EitherT(api.downloadFileById(fileId, authorizeAccountResponse.apiUrl,
          authorizeAccountResponse.authorizationToken.some))
    } yield download

    tryAgainIfExpired(result.value) {
      authorizeAccountPromise = Promise()
      downloadById(fileId)
    }
  }

  def downloadByName(fileName: FileName, bucketName: BucketName): B2Response[DownloadFileResponse] = {
    import cats.implicits._
    val result = for {
      authorizeAccountResponse <- EitherT(obtainAuthorizeAccountResponse())
      download <- EitherT(
          api.downloadFileByName(
            fileName,
            bucketName,
            authorizeAccountResponse.apiUrl,
            authorizeAccountResponse.authorizationToken.some
          ))
    } yield download

    tryAgainIfExpired(result.value) {
      authorizeAccountPromise = Promise()
      downloadByName(fileName, bucketName)
    }
  }

  def deleteFileVersion(fileVersionInfo: FileVersionInfo): B2Response[FileVersionInfo] = {
    import cats.implicits._
    val result = for {
      authorizeAccountResponse <- EitherT(obtainAuthorizeAccountResponse())
      delete <- EitherT(api.deleteFileVersion(fileVersionInfo, authorizeAccountResponse.apiUrl,
          authorizeAccountResponse.authorizationToken))
    } yield delete

    tryAgainIfExpired(result.value) {
      authorizeAccountPromise = Promise()
      deleteFileVersion(fileVersionInfo)
    }
  }
}
