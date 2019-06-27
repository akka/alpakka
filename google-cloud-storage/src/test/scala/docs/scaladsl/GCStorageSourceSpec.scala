/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.http.scaladsl.model.ContentTypes
import akka.stream.alpakka.googlecloud.storage.scaladsl.{GCStorage, GCStorageWiremockBase}
import akka.stream.alpakka.googlecloud.storage.{Bucket, GCStorageAttributes, GCStorageExt, StorageObject}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Attributes}
import akka.util.ByteString
import org.scalatest._
import org.scalatest.concurrent._

import scala.concurrent.Future

class GCStorageSourceSpec
    extends GCStorageWiremockBase
    with WordSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with Matchers {

  implicit val materializer = ActorMaterializer()
  private val sampleSettings = GCStorageExt(system).settings

  override def beforeAll(): Unit =
    mockTokenApi()

  override protected def afterAll(): Unit = {
    super.afterAll()
    this.stopWireMockServer()
  }

  "GCStorageSource" should {
    "create a bucket" in {
      val location = "europe-west1"

      mockBucketCreate(location)

      // #make-bucket

      implicit val sampleAttributes: Attributes = GCStorageAttributes.settings(sampleSettings)

      val createBucketResponse: Future[Bucket] = GCStorage.createBucket(bucketName, location)
      val createBucketSourceResponse: Source[Bucket, NotUsed] = GCStorage.createBucketSource(bucketName, location)

      // #make-bucket

      createBucketResponse.futureValue.kind shouldBe "storage#bucket"
      createBucketResponse.futureValue.name shouldBe bucketName
      createBucketResponse.futureValue.location shouldBe location.toUpperCase

      val bucket = createBucketSourceResponse.runWith(Sink.head)
      bucket.futureValue.kind shouldBe "storage#bucket"
      bucket.futureValue.name shouldBe bucketName
      bucket.futureValue.location shouldBe location.toUpperCase

    }

    "fail with error when bucket creation fails" in {
      val location = "europe-west1"

      mockBucketCreateFailure(location)

      implicit val sampleAttributes: Attributes = GCStorageAttributes.settings(sampleSettings)

      val createBucketResponse = GCStorage.createBucket(bucketName, location)
      val createBucketSourceResponse = GCStorage.createBucketSource(bucketName, location)

      createBucketResponse.failed.futureValue.getMessage shouldBe "[400] Create failed"
      createBucketSourceResponse.runWith(Sink.head).failed.futureValue.getMessage shouldBe "[400] Create failed"
    }

    "delete a bucket" in {

      mockDeleteBucket()

      //#delete-bucket

      implicit val sampleAttributes: Attributes = GCStorageAttributes.settings(sampleSettings)

      val deleteBucketResponse: Future[Done] = GCStorage.deleteBucket(bucketName)
      val deleteBucketSourceResponse: Source[Done, NotUsed] = GCStorage.deleteBucketSource(bucketName)

      //#delete-bucket

      deleteBucketResponse.futureValue shouldBe Done
      deleteBucketSourceResponse.runWith(Sink.ignore).futureValue shouldBe Done
    }

    "fail with error when bucket deletion fails" in {

      mockDeleteBucketFailure()

      implicit val sampleAttributes: Attributes = GCStorageAttributes.settings(sampleSettings)

      val deleteBucketResponse = GCStorage.deleteBucket(bucketName)
      val deleteBucketSourceResponse = GCStorage.deleteBucketSource(bucketName)

      deleteBucketResponse.failed.futureValue.getMessage shouldBe "[400] Delete failed"
      deleteBucketSourceResponse.runWith(Sink.ignore).failed.futureValue.getMessage shouldBe "[400] Delete failed"
    }

    "get a bucket if bucket exists" in {

      mockGetExistingBucket()

      //#get-bucket

      implicit val sampleAttributes: Attributes = GCStorageAttributes.settings(sampleSettings)

      val getBucketResponse: Future[Option[Bucket]] = GCStorage.getBucket(bucketName)
      val getBucketSourceResponse: Source[Option[Bucket], NotUsed] = GCStorage.getBucketSource(bucketName)

      //#get-bucket

      val bucket = getBucketResponse.futureValue
      bucket.isDefined shouldBe true
      bucket.map(_.name) shouldBe Some(bucketName)

      val bucketFromSource = getBucketSourceResponse.runWith(Sink.head).futureValue
      bucketFromSource.isDefined shouldBe true
      bucketFromSource.map(_.name) shouldBe Some(bucketName)
    }

    "not return bucket if bucket doesn't exist" in {

      mockGetNonExistingBucket()

      implicit val sampleAttributes: Attributes = GCStorageAttributes.settings(sampleSettings)

      val getBucketResponse = GCStorage.getBucket(bucketName)
      val getBucketSourceResponse = GCStorage.getBucketSource(bucketName)

      getBucketResponse.futureValue shouldBe None
      getBucketSourceResponse.runWith(Sink.head).futureValue shouldBe None
    }

    "fail with error when getting a bucket fails" in {

      mockGetBucketFailure()

      implicit val sampleAttributes: Attributes = GCStorageAttributes.settings(sampleSettings)

      val getBucketResponse = GCStorage.getBucket(bucketName)
      val getBucketSourceResponse = GCStorage.getBucketSource(bucketName)

      getBucketResponse.failed.futureValue.getMessage shouldBe "[400] Get bucket failed"
      getBucketSourceResponse.runWith(Sink.ignore).failed.futureValue.getMessage shouldBe "[400] Get bucket failed"
    }

    "list an empty bucket" in {

      mockEmptyBucketListing()

      val listSource = GCStorage.listBucket(bucketName, None)

      listSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "list non existing folder" in {
      val folder = "folder"
      mockNonExistingFolderListing(folder)

      val listSource = GCStorage.listBucket(bucketName, Some(folder))

      listSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "list a non existing bucket" in {
      mockNonExistingBucketListing()

      val listSource = GCStorage.listBucket(bucketName, None)

      listSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "list an existing bucket using multiple requests" in {
      val firstFileName = "file1.txt"
      val secondFileName = "file2.txt"

      mockBucketListing(firstFileName, secondFileName)

      val listSource = GCStorage.listBucket(bucketName, None)

      listSource.runWith(Sink.seq).futureValue.map(_.name) shouldBe Seq(firstFileName, secondFileName)
    }

    "list an folder in existing bucket using multiple requests" in {
      val firstFileName = "file1.txt"
      val secondFileName = "file2.txt"
      val folder = "folder"

      mockBucketListing(firstFileName, secondFileName, Some(folder))

      //#list-bucket

      val listSource: Source[StorageObject, NotUsed] = GCStorage.listBucket(bucketName, Some(folder))

      //#list-bucket

      listSource.runWith(Sink.seq).futureValue.map(_.name) shouldBe Seq(firstFileName, secondFileName)
    }

    "fail with error when bucket listing fails" in {

      mockBucketListingFailure()

      val listSource = GCStorage.listBucket(bucketName, None)

      listSource.runWith(Sink.seq).failed.futureValue.getMessage shouldBe "[400] Bucket listing failed"
    }

    "return empty source listing bucket with wrong settings" in {

      mockBucketListingFailure()

      //#list-bucket-attributes

      val newBasePathSettings = GCStorageExt(this.system).settings.withBasePath("/storage/v1")

      val listSource: Source[StorageObject, NotUsed] =
        GCStorage.listBucket(bucketName, None).withAttributes(GCStorageAttributes.settings(newBasePathSettings))

      //#list-bucket-attributes

      listSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "get existing storage object" in {

      mockGetExistingStorageObject()

      //#objectMetadata

      val getObjectSource: Source[Option[StorageObject], NotUsed] = GCStorage.getObject(bucketName, fileName)

      //#objectMetadata

      val result = getObjectSource.runWith(Sink.head).futureValue

      result.map(_.name) shouldBe Some(fileName)
      result.map(_.bucket) shouldBe Some(bucketName)
    }

    "get None if storage object doesn't exist" in {

      mockGetNonExistingStorageObject()

      val getObjectSource = GCStorage.getObject(bucketName, fileName)

      getObjectSource.runWith(Sink.head).futureValue shouldBe None
    }

    "fail with error when get storage object fails" in {

      mockGetNonStorageObjectFailure()

      val getObjectSource = GCStorage.getObject(bucketName, fileName)

      getObjectSource.runWith(Sink.head).failed.futureValue.getMessage shouldBe "[400] Get storage object failed"
    }

    "download file when file exists" in {
      val fileContent = "Google storage file content"

      mockFileDownload(fileContent)

      //#download

      val downloadSource: Source[Option[Source[ByteString, NotUsed]], NotUsed] =
        GCStorage.download(bucketName, fileName)

      val Some(data: Source[ByteString, _]): Option[Source[ByteString, NotUsed]] =
        downloadSource.runWith(Sink.head).futureValue

      val result: Future[Seq[String]] = data.map(_.utf8String).runWith(Sink.seq)

      //#download

      result.futureValue.mkString shouldBe fileContent

    }

    "download results in None when file doesn't exist" in {
      val bucketName = "alpakka"
      val fileName = "file1.txt"

      mockNonExistingFileDownload()

      val downloadSource = GCStorage.download(bucketName, fileName)

      downloadSource.runWith(Sink.head).futureValue shouldBe None
    }

    "fail with error when file download fails" in {
      val bucketName = "alpakka"
      val fileName = "file1.txt"

      mockFileDownloadFailure()

      val downloadSource = GCStorage.download(bucketName, fileName)
      downloadSource
        .runWith(Sink.head)
        .failed
        .futureValue
        .getMessage shouldBe "[400] File download failed"
    }

    "upload small file" in {
      val fileContent = "chunk1"
      val contentType = ContentTypes.`application/octet-stream`
      val fileSource = Source.single(ByteString(fileContent))

      mockUploadSmallFile(fileContent)

      val simpleUploadSource = GCStorage.simpleUpload(bucketName, fileName, fileSource, contentType)

      val storageObject = simpleUploadSource.runWith(Sink.head).futureValue

      storageObject.name shouldBe fileName
      storageObject.bucket shouldBe bucketName
    }

    "fail with error when small file upload fails" in {
      val fileContent = "chunk1"
      val contentType = ContentTypes.`application/octet-stream`
      val fileSource = Source.single(ByteString(fileContent))

      mockUploadSmallFileFailure(fileContent)

      val simpleUploadSource = GCStorage.simpleUpload(bucketName, fileName, fileSource, contentType)

      simpleUploadSource.runWith(Sink.head).failed.futureValue.getMessage shouldBe "[400] Upload small file failed"
    }

    "delete existing object" in {

      mockDeleteObject(fileName)

      val deleteSource = GCStorage.deleteObject(bucketName, fileName)
      deleteSource.runWith(Sink.head).futureValue shouldBe true
    }

    "not delete non existing object" in {

      mockNonExistingDeleteObject(fileName)

      val deleteSource = GCStorage.deleteObject(bucketName, fileName)
      deleteSource.runWith(Sink.head).futureValue shouldBe false
    }

    "fail when delete object fails" in {

      mockDeleteObjectFailure(fileName)

      val deleteSource = GCStorage.deleteObject(bucketName, fileName)

      deleteSource.runWith(Sink.ignore).failed.futureValue.getMessage shouldBe "[400] Delete object failed"
    }

    "delete existing folder" in {
      val firstFileName = "file1.txt"
      val secondFileName = "file2.txt"
      val prefix = "folder"

      mockBucketListing(firstFileName, secondFileName, Some(prefix))
      mockDeleteObject(firstFileName)
      mockDeleteObject(secondFileName)

      val deleteObjectsByPrefixSource = GCStorage.deleteObjectsByPrefix(bucketName, Some(prefix))
      deleteObjectsByPrefixSource.runWith(Sink.seq).futureValue shouldBe Seq(true, true)
    }

    "not delete non existing folder" in {
      val prefix = "folder"

      mockNonExistingBucketListing(Some(prefix))
      mockObjectDoesNotExist(prefix)

      val deleteObjectsByPrefixSource = GCStorage.deleteObjectsByPrefix(bucketName, Some(prefix))

      deleteObjectsByPrefixSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "fail when folder delete fails" in {
      val firstFileName = "file1.txt"
      val secondFileName = "file2.txt"
      val prefix = "folder"

      mockBucketListing(firstFileName, secondFileName, Some(prefix))
      mockDeleteObject(firstFileName)
      mockDeleteObject(secondFileName)
      mockDeleteObjectFailure(secondFileName)

      val deleteObjectsByPrefixSource = GCStorage.deleteObjectsByPrefix(bucketName, Some(prefix))
      deleteObjectsByPrefixSource.runWith(Sink.seq).failed.futureValue.getMessage shouldBe "[400] Delete object failed"
    }
  }
}

object TestCredentials {
  val accessToken =
    "ya29.Elz4A2XkfGKJ4CoS5x_umUBHsvjGdeWQzu6gRRCnNXI0fuIyoDP_6aYktBQEOI4YAhLNgUl2OpxWQaN8Z3hd5YfFw1y4EGAtr2o28vSID-c8ul_xxHuudE7RmhH9sg"
}
