/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.stream.scaladsl.Flow
import cats.syntax.either._

/**
 * Warning - the returned flows are not thread safe.
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
class B2Streams(accountCredentials: B2AccountCredentials)(implicit val system: ActorSystem,
                                                          materializer: ActorMaterializer) {
  implicit val executionContext = materializer.executionContext
  private val parallelism = 1

  private def createClient(bucketId: BucketId) = new B2Client(accountCredentials, bucketId)

  /**
   * Warning: The returned flow is not thread safe
   */
  def uploadFiles(bucketId: BucketId): Flow[UploadFileRequest, UploadFileResponse, NotUsed] = {
    val client = createClient(bucketId)
    Flow[UploadFileRequest].mapAsyncUnordered(parallelism) { x =>
      client.upload(x.fileName, x.data, x.contentType).map(_ getOrElse sys.error(s"Failed to upload $x"))
    }
  }

  /**
   * Warning: The returned flow is not thread safe
   */
  def downloadFilesById(bucketId: BucketId): Flow[FileId, DownloadFileResponse, NotUsed] = {
    val client = createClient(bucketId)
    Flow[FileId].mapAsyncUnordered(parallelism) { x =>
      client.downloadById(x).map(_ getOrElse sys.error(s"Failed to download $x"))
    }
  }

  /**
   * Warning: The returned flow is not thread safe
   */
  def deleteFileVersions(bucketId: BucketId): Flow[FileVersionInfo, FileVersionInfo, NotUsed] = {
    val client = createClient(bucketId)
    Flow[FileVersionInfo].mapAsyncUnordered(parallelism) { x =>
      client.deleteFileVersion(x).map(_ getOrElse sys.error(s"Failed to delete $x"))
    }
  }
}
