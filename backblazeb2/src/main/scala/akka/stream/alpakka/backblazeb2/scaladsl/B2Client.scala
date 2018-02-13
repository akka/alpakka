/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.http.scaladsl.model.ContentType
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.util.ByteString
import cats.data.EitherT
import cats.syntax.option._
import scala.concurrent.{Future, Promise}
import RetryUtils._

/**
 * Warning - Not thread safe.
 */
private[scaladsl] class B2Client(
    api: B2API,
    accountAuthorizer: B2AccountAuthorizer,
    bucketId: BucketId
)(implicit materializer: ActorMaterializer) {
  implicit val executionContext = materializer.executionContext

  @volatile private var getUploadUrlPromise: Promise[GetUploadUrlResponse] = Promise()

  /**
   * Return the saved GetUploadUrlResponse if it exists or obtain a new one if it doesn't
   */
  private def obtainGetUploadUrlResponse(): B2Response[GetUploadUrlResponse] =
    returnOrObtain(getUploadUrlPromise, callGetUploadUrl _)

  private def callGetUploadUrl(): B2Response[GetUploadUrlResponse] =
    accountAuthorizer.withAuthorization { authorizeAccountResponse =>
      api.getUploadUrl(authorizeAccountResponse, bucketId)
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
    accountAuthorizer.withAuthorization { authorizeAccountResponse =>
      api.downloadFileById(fileId, authorizeAccountResponse.apiUrl, authorizeAccountResponse.authorizationToken.some)
    }

  def downloadByName(fileName: FileName, bucketName: BucketName): B2Response[DownloadFileResponse] =
    accountAuthorizer.withAuthorization { authorizeAccountResponse =>
      api.downloadFileByName(
        fileName,
        bucketName,
        authorizeAccountResponse.apiUrl,
        authorizeAccountResponse.authorizationToken.some
      )
    }

  def deleteFileVersion(fileVersionInfo: FileVersionInfo): B2Response[FileVersionInfo] =
    accountAuthorizer.withAuthorization { authorizeAccountResponse =>
      api.deleteFileVersion(fileVersionInfo,
                            authorizeAccountResponse.apiUrl,
                            authorizeAccountResponse.authorizationToken)
    }

  def listFileVersions(fileName: FileName): B2Response[ListFileVersionsResponse] =
    accountAuthorizer.withAuthorization { authorizeAccountResponse =>
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
          )
        )

      case Right(x) =>
        deleteFileVersions(x.files)
    }
  }
}
