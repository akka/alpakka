/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.scaladsl.B2Client
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class B2ClientIntegrationSpec extends FlatSpec with B2IntegrationTest {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val client = new B2Client(credentials, bucketId)

  it should "handle happy path" in {
    val uploadResultF = client.upload(fileName, dataByteString)
    val uploadResult = extractFromResponse(uploadResultF)

    val downloadByNameResultF = client.downloadByName(uploadResult.fileName, bucketName)
    val downloadByNameResult = extractFromResponse(downloadByNameResultF)

    checkDownloadResponse(downloadByNameResult)

    val downloadByIdResultF = client.downloadById(uploadResult.fileId)
    val downloadByIdResult = extractFromResponse(downloadByIdResultF)

    checkDownloadResponse(downloadByIdResult)

    val deleteFileResultF = client.deleteFileVersion(downloadByIdResult.fileVersion)
    val deleteFileResult = extractFromResponse(deleteFileResultF)

    deleteFileResult.fileId shouldEqual uploadResult.fileId
    deleteFileResult.fileName shouldEqual fileName
  }
}
