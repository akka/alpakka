/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.http.scaladsl.model.ContentTypes
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.storage.StorageObject
import akka.stream.alpakka.googlecloud.storage.scaladsl.{GCStorage, GCStorageWiremockBase}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future
import scala.util.Random

class GCStorageSinkSpec
    extends GCStorageWiremockBase
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with Matchers {

  implicit val materializer = ActorMaterializer()

  override def beforeAll(): Unit =
    mockTokenApi()

  override protected def afterAll(): Unit = {
    super.afterAll()
    this.stopWireMockServer()
  }

  "upload large file" in {
    val chunkSize = 256 * 1024
    val firstChunkContent = Random.alphanumeric.take(chunkSize).mkString
    val secondChunkContent = Random.alphanumeric.take(chunkSize).mkString

    mockLargeFileUpload(firstChunkContent, secondChunkContent, chunkSize)

    //#upload
    val sink =
      GCStorage.resumableUpload(bucketName, fileName, ContentTypes.`text/plain(UTF-8)`, chunkSize)

    val source = Source(
      List(ByteString(firstChunkContent), ByteString(secondChunkContent))
    )

    val result: Future[StorageObject] = source.runWith(sink)

    //#upload

    val storageObject: StorageObject = result.futureValue

    storageObject.name shouldBe fileName
    storageObject.bucket shouldBe bucketName
  }

  "fail with error when large file upload fails" in {
    val chunkSize = 256 * 1024
    val firstChunkContent = Random.alphanumeric.take(chunkSize).mkString
    val secondChunkContent = Random.alphanumeric.take(chunkSize).mkString

    mockLargeFileUploadFailure(firstChunkContent, secondChunkContent, chunkSize)

    val sink =
      GCStorage.resumableUpload(bucketName, fileName, ContentTypes.`text/plain(UTF-8)`, chunkSize)

    val source = Source(List(ByteString(firstChunkContent), ByteString(secondChunkContent)))
    source
      .runWith(sink)
      .failed
      .futureValue
      .getMessage shouldBe "Uploading part failed with status 400 Bad Request: Chunk upload failed"
  }

  "rewrite file" in {
    val rewriteBucketName = "alpakka-rewrite"

    mockRewrite(rewriteBucketName)

    // #rewrite

    val result: Future[StorageObject] = GCStorage.rewrite(bucketName, fileName, rewriteBucketName, fileName).run

    // #rewrite

    val storageObject = result.futureValue

    storageObject.name shouldBe fileName
    storageObject.bucket shouldBe rewriteBucketName
  }

  "fail when rewrite file fails" in {
    val rewriteBucketName = "alpakka-rewrite"

    mockRewriteFailure(rewriteBucketName)

    val result = GCStorage.rewrite(bucketName, fileName, rewriteBucketName, fileName).run

    result.failed.futureValue.getMessage shouldBe "[400] Rewrite failed"
  }

}
