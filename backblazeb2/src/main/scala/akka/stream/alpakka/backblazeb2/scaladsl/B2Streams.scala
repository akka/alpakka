package akka.stream.alpakka.backblazeb2.scaladsl

import akka.NotUsed
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.stream.scaladsl.Flow

/**
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
class B2Streams(credentials: B2AccountCredentials) {
  def uploadFiles(bucketId: BucketId): Flow[UploadFileRequest, UploadFileResponse, NotUsed] = {
    ???
  }

  def downloadFilesById(bucketId: BucketId): Flow[FileId, DownloadFileByIdResponse, NotUsed] = {
    ???
  }
}
