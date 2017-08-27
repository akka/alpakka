/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol.{DeleteAllFileVersionsResponse, FileVersionInfo}
import akka.stream.alpakka.backblazeb2.scaladsl.B2Client
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.concurrent.Await

class B2ClientIntegrationSpec extends FlatSpec with B2IntegrationTest {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val client = new B2Client(credentials, bucketId, eagerAuthorization = true)

  it should "handle happy path by id" in {
    val uploadResultF = client.upload(fileName, dataByteString)
    val uploadResult = extractFromResponse(uploadResultF)

    val downloadByIdResultF = client.downloadById(uploadResult.fileId)
    val downloadByIdResult = extractFromResponse(downloadByIdResultF)

    checkDownloadResponse(downloadByIdResult)

    val deleteFileResultF = client.deleteFileVersion(downloadByIdResult.fileVersion)
    val deleteFileResult = extractFromResponse(deleteFileResultF)

    deleteFileResult.fileId shouldEqual uploadResult.fileId
    deleteFileResult.fileName shouldEqual fileName
  }

  it should "handle happy path by name" in {
    val uploadResultF = client.upload(fileName, dataByteString)
    val uploadResult = extractFromResponse(uploadResultF)

    uploadResult.fileName shouldEqual fileName

    val downloadByNameResultF = client.downloadByName(fileName, bucketName)
    val downloadByNameResult = extractFromResponse(downloadByNameResultF)

    checkDownloadResponse(downloadByNameResult)

    val deleteFileVersionsResultF = client.deleteAllFileVersions(fileName)
    val deleteFileVersionsResult = Await.result(deleteFileVersionsResultF, timeout)

    deleteFileVersionsResult shouldEqual DeleteAllFileVersionsResponse(
      successes = FileVersionInfo(fileName, uploadResult.fileId) :: Nil,
      failures = Nil
    )
  }
}
