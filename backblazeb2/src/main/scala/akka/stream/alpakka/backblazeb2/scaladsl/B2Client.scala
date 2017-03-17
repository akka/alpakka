package akka.stream.alpakka.backblazeb2.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.util.ByteString
import cats.data.EitherT
import cats.syntax.either._

import scala.concurrent.{Future, Promise}

/**
  * Warning - Not thread safe.
  *
  * Uploading in Parallel https://www.backblaze.com/b2/docs/uploading.html
  *
  * The URL and authorization token that you get from b2_get_upload_url can be used by only one thread at a time. If you want multiple threads running, each one needs to get its own URL and auth token. It can keep using that URL and auth token for multiple uploads, until it gets a returned status indicating that it should get a new upload URL.
  *
  *
  * Multithreading uploads https://www.backblaze.com/b2/docs/integration_checklist.html
  *
  * If your tool is uploading many files, the best performance will be achieved by multithreading the upload and launching multiple threads simultaneously.
  * When initializing upload threads, call b2_get_upload_url or b2_get_upload_part_url for each thread. The returned upload URL / upload authorization token should be used in this thread to upload files until one of the error conditions described above occurs. When that happens, simply call b2_get_upload_url or b2_get_upload_part_url again and resume the uploads.
  */
class B2Client(accountCredentials: B2AccountCredentials, bucketId: BucketId, hostAndPort: String = B2API.DefaultHostAndPort)(implicit system: ActorSystem, materializer: ActorMaterializer) {
  implicit val executionContext = materializer.executionContext
  private val api = new B2API(hostAndPort)

  private var authorizeAccountResponse: Promise[AuthorizeAccountResponse] = Promise()
  private val getUploadUrlResponse: Promise[GetUploadUrlResponse] = Promise()

  /**
    * Return the saved AuthorizeAccountResponse if it exists or obtain a new one if it doesn't
    */
  private def obtainAuthorizeAccountResponse(): B2Response[AuthorizeAccountResponse] = {
    returnOrObtain(authorizeAccountResponse, callAuthorizeAccount)
  }

  /**
    * Return the saved GetUploadUrlResponse if it exists or obtain a new one if it doesn't
    */
  private def obtainGetUploadUrlResponse(): B2Response[GetUploadUrlResponse] = {
    returnOrObtain(getUploadUrlResponse, callGetUploadUrl) flatMap {
      case Left(error) if error.isExpiredToken =>
        authorizeAccountResponse = Promise()
        callGetUploadUrl()

      case success: Right[B2Error, GetUploadUrlResponse] =>
        Future.successful(success)
    }
  }

  private def callAuthorizeAccount(): B2Response[AuthorizeAccountResponse] = {
    api.authorizeAccount(accountCredentials)
  }

  private def callGetUploadUrl(): B2Response[GetUploadUrlResponse] = {
    import cats.implicits._
    val eitherT = for {
      authorizeAccountResponse <- EitherT(obtainAuthorizeAccountResponse())
      response <- EitherT(api.getUploadUrl(authorizeAccountResponse, bucketId))
    } yield response

    eitherT.value
  }

  private def returnOrObtain[T](promise: Promise[T], f: () => B2Response[T]): B2Response[T] = {
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
  }

  def upload(fileName: FileName, data: ByteString): B2Response[UploadFileResponse] = {
    import cats.implicits._
    val result = for {
      getUploadUrlResponse <- EitherT(obtainGetUploadUrlResponse())
      upload <- EitherT(api.uploadFile(getUploadUrlResponse, fileName, data))
    } yield upload

    result.value // TODO: handle expired auth token
  }
}
