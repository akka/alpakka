package akka.stream.alpakka.backblazeb2.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.util.ByteString
import cats.syntax.option._

import scala.concurrent.{Future, Promise}

/**
  * Not thread safe.
  */
/*
 * TODO - Thread safety / Multithreading uploads
 *
 * Uploading in Parallel https://www.backblaze.com/b2/docs/uploading.html
 * The URL and authorization token that you get from b2_get_upload_url can be used by only one thread at a time. If you want multiple threads running, each one needs to get its own URL and auth token. It can keep using that URL and auth token for multiple uploads, until it gets a returned status indicating that it should get a new upload URL.
 *
 * Multithreading uploads https://www.backblaze.com/b2/docs/integration_checklist.html
 * If your tool is uploading many files, the best performance will be achieved by multithreading the upload and launching multiple threads simultaneously.
 * When initializing upload threads, call b2_get_upload_url or b2_get_upload_part_url for each thread. The returned upload URL / upload authorization token should be used in this thread to upload files until one of the error conditions described above occurs. When that happens, simply call b2_get_upload_url or b2_get_upload_part_url again and resume the uploads.
 */
class B2Client(accountCredentials: B2AccountCredentials, bucketId: BucketId, hostAndPort: String = B2API.DefaultHostAndPort)(implicit system: ActorSystem, materializer: ActorMaterializer) {
  implicit val executionContext = materializer.executionContext
  private val api = new B2API(hostAndPort)

  private val authorizeAccountResponse: Promise[AuthorizeAccountResponse] = Promise()
  private val getUploadUrlResponse: Promise[GetUploadUrlResponse] = Promise()

  /**
    * Return the saved AuthorizeAccountResponse if it exists or obtain a new one if it doesn't
    */
  private def obtainAuthorizeAccountResponse(): Future[AuthorizeAccountResponse] = {
    if (authorizeAccountResponse.isCompleted) {
      authorizeAccountResponse.future
    } else {
      val result = api.authorizeAccount(accountCredentials)
      authorizeAccountResponse.completeWith(result)
      result
    }
  }

  /**
    * Return the saved GetUploadUrlResponse if it exists or obtain a new one if it doesn't
    */
  private def obtainGetUploadUrlResponse(): Future[GetUploadUrlResponse] = {
    if (getUploadUrlResponse.isCompleted) {
      getUploadUrlResponse.future
    } else {
      obtainAuthorizeAccountResponse() flatMap { authorizeAccountResponse =>
        val result = api.getUploadUrl(authorizeAccountResponse, bucketId)
        getUploadUrlResponse.completeWith(result)
        result
      }
    }
  }

  def upload(fileName: FileName, data: ByteString): Future[UploadFileResponse] = {
    val getUploadUrlResponse = obtainGetUploadUrlResponse()

    getUploadUrlResponse flatMap { getUploadUrlResponse =>
      api.uploadFile(getUploadUrlResponse, fileName, data)
    }
  }
}
