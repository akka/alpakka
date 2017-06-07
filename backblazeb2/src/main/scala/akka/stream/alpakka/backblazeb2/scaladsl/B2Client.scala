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
import cats.syntax.option._
import scala.concurrent.{Future, Promise}

/**
 * Warning - Not thread safe.
 */
class B2Client(
    accountCredentials: B2AccountCredentials,
    bucketId: BucketId,
    eagerAuthorization: Boolean,
    hostAndPort: String = B2API.DefaultHostAndPort)(implicit system: ActorSystem, materializer: ActorMaterializer) {
  implicit val executionContext = materializer.executionContext
  private val api = new B2API(hostAndPort)

  private var authorizeAccountPromise: Promise[AuthorizeAccountResponse] = Promise()
  private var getUploadUrlPromise: Promise[GetUploadUrlResponse] = Promise()

  if (eagerAuthorization) { // if authorization is eager, let us start with this upon construction
    val _ = obtainAuthorizeAccountResponse()
  }

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

  private def withAuthorization[T](f: AuthorizeAccountResponse => B2Response[T]): B2Response[T] = {
    import cats.implicits._
    val result = for {
      authorizeAccountResponse <- EitherT(obtainAuthorizeAccountResponse())
      operation <- EitherT(f(authorizeAccountResponse))
    } yield operation

    tryAgainIfExpired(result.value) {
      authorizeAccountPromise = Promise()
      withAuthorization(f)
    }
  }

  private def callGetUploadUrl(): B2Response[GetUploadUrlResponse] =
    withAuthorization { authorizeAccountResponse =>
      api.getUploadUrl(authorizeAccountResponse, bucketId)
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

  def downloadById(fileId: FileId): B2Response[DownloadFileResponse] =
    withAuthorization { authorizeAccountResponse =>
      api.downloadFileById(fileId, authorizeAccountResponse.apiUrl, authorizeAccountResponse.authorizationToken.some)
    }

  def downloadByName(fileName: FileName, bucketName: BucketName): B2Response[DownloadFileResponse] =
    withAuthorization { authorizeAccountResponse =>
      api.downloadFileByName(
        fileName,
        bucketName,
        authorizeAccountResponse.apiUrl,
        authorizeAccountResponse.authorizationToken.some
      )
    }

  def deleteFileVersion(fileVersionInfo: FileVersionInfo): B2Response[FileVersionInfo] =
    withAuthorization { authorizeAccountResponse =>
      api.deleteFileVersion(fileVersionInfo, authorizeAccountResponse.apiUrl,
        authorizeAccountResponse.authorizationToken)
    }

  def listFileVersions(fileName: FileName): B2Response[ListFileVersionsResponse] =
    withAuthorization { authorizeAccountResponse =>
      api.listFileVersions(
        bucketId,
        startFileId = None,
        startFileName = fileName.some,
        maxFileCount = 1,
        apiUrl = authorizeAccountResponse.apiUrl,
        accountAuthorization = authorizeAccountResponse.authorizationToken
      )
    }

  def deleteAllFileVersions(fileName: FileName): Future[DeleteAllFileVersionsResponse] = {
    def deleteFileVersions(versions: List[FileVersionInfo]): Future[DeleteAllFileVersionsResponse] = {
      val deletionFutures = versions map { file =>
        deleteFileVersion(file)
      }

      Future.sequence(deletionFutures).map { data =>
        val (lefts, rights) = data.partition(_.isLeft)
        val failures = lefts.collect {
          case Left(x) => x
        }
        val successes = rights.collect {
          case Right(x) => x
        }
        DeleteAllFileVersionsResponse(successes = successes, failures = failures)
      }
    }

    listFileVersions(fileName) flatMap {
      case Left(x) =>
        Future.successful(
            DeleteAllFileVersionsResponse(
              failures = x :: Nil,
              successes = Nil
            ))

      case Right(x) =>
        deleteFileVersions(x.files)
    }
  }
}
