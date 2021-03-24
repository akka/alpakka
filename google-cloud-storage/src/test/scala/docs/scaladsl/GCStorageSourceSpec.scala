/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.http.scaladsl.model.ContentTypes
import akka.stream.alpakka.googlecloud.storage.scaladsl.{GCStorage, GCStorageWiremockBase}
import akka.stream.alpakka.googlecloud.storage.{Bucket, StorageObject}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.Attributes
import akka.stream.alpakka.google.{GoogleAttributes, GoogleSettings}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent._

import scala.concurrent.Future

class GCStorageSourceSpec
    extends GCStorageWiremockBase
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with Matchers
    with LogCapturing {

  private val sampleSettings = GoogleSettings()

  override def beforeAll(): Unit = ()

  override protected def afterAll(): Unit = {
    super.afterAll()
    this.stopWireMockServer()
  }

  "GCStorageSource" should {
    "create a bucket" in {
      val location = "europe-west1"

      mock.simulate(
        mockTokenApi,
        mockBucketCreate(location)
      )

      // #make-bucket

      implicit val sampleAttributes: Attributes = GoogleAttributes.settings(sampleSettings)

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

      mock.simulate(
        mockTokenApi,
        mockBucketCreateFailure(location)
      )

      implicit val sampleAttributes: Attributes = GoogleAttributes.settings(sampleSettings)

      val createBucketResponse = GCStorage.createBucket(bucketName, location)
      val createBucketSourceResponse = GCStorage.createBucketSource(bucketName, location)

      createBucketResponse.failed.futureValue.getMessage shouldBe "[400] Create failed"
      createBucketSourceResponse.runWith(Sink.head).failed.futureValue.getMessage shouldBe "[400] Create failed"
    }

    "delete a bucket" in {

      mock.simulate(
        mockTokenApi,
        mockDeleteBucket()
      )

      //#delete-bucket

      implicit val sampleAttributes: Attributes = GoogleAttributes.settings(sampleSettings)

      val deleteBucketResponse: Future[Done] = GCStorage.deleteBucket(bucketName)
      val deleteBucketSourceResponse: Source[Done, NotUsed] = GCStorage.deleteBucketSource(bucketName)

      //#delete-bucket

      deleteBucketResponse.futureValue shouldBe Done
      deleteBucketSourceResponse.runWith(Sink.ignore).futureValue shouldBe Done
    }

    "fail with error when bucket deletion fails" in {

      mock.simulate(
        mockTokenApi,
        mockDeleteBucketFailure()
      )

      implicit val sampleAttributes: Attributes = GoogleAttributes.settings(sampleSettings)

      val deleteBucketResponse = GCStorage.deleteBucket(bucketName)
      val deleteBucketSourceResponse = GCStorage.deleteBucketSource(bucketName)

      deleteBucketResponse.failed.futureValue.getMessage shouldBe "[400] Delete failed"
      deleteBucketSourceResponse.runWith(Sink.ignore).failed.futureValue.getMessage shouldBe "[400] Delete failed"
    }

    "get a bucket if bucket exists" in {

      mock.simulate(
        mockTokenApi,
        mockGetExistingBucket()
      )

      //#get-bucket

      implicit val sampleAttributes: Attributes = GoogleAttributes.settings(sampleSettings)

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

      mock.simulate(
        mockTokenApi,
        mockGetNonExistingBucket()
      )

      implicit val sampleAttributes: Attributes = GoogleAttributes.settings(sampleSettings)

      val getBucketResponse = GCStorage.getBucket(bucketName)
      val getBucketSourceResponse = GCStorage.getBucketSource(bucketName)

      getBucketResponse.futureValue shouldBe None
      getBucketSourceResponse.runWith(Sink.head).futureValue shouldBe None
    }

    "fail with error when getting a bucket fails" in {

      mock.simulate(
        mockTokenApi,
        mockGetBucketFailure()
      )

      implicit val sampleAttributes: Attributes = GoogleAttributes.settings(sampleSettings)

      val getBucketResponse = GCStorage.getBucket(bucketName)
      val getBucketSourceResponse = GCStorage.getBucketSource(bucketName)

      getBucketResponse.failed.futureValue.getMessage shouldBe "[400] Get bucket failed"
      getBucketSourceResponse.runWith(Sink.ignore).failed.futureValue.getMessage shouldBe "[400] Get bucket failed"
    }

    "list an empty bucket" in {

      mock.simulate(
        mockTokenApi,
        mockEmptyBucketListing()
      )

      val listSource = GCStorage.listBucket(bucketName, None)

      listSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "list non existing folder" in {
      val folder = "folder"
      mock.simulate(
        mockTokenApi,
        mockNonExistingFolderListing(folder)
      )

      val listSource = GCStorage.listBucket(bucketName, Some(folder))

      listSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "list a non existing bucket" in {
      mock.simulate(
        mockTokenApi,
        mockNonExistingBucketListing()
      )

      val listSource = GCStorage.listBucket(bucketName, None)

      listSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "list an existing bucket using multiple requests" in {
      val firstFileName = "file1.txt"
      val secondFileName = "file2.txt"
      val versions = true

      mock.simulate(
        mockTokenApi,
        mockBucketListing(firstFileName, secondFileName),
        mockBucketListing(firstFileName, secondFileName, None, versions)
      )

      val listSource = GCStorage.listBucket(bucketName, None)
      val listVersionsSource = GCStorage.listBucket(bucketName, None, versions)

      listSource.runWith(Sink.seq).futureValue.map(_.name) shouldBe Seq(firstFileName, secondFileName)
      listVersionsSource.runWith(Sink.seq).futureValue.map(_.name) shouldBe
      Seq(firstFileName, s"$firstFileName#$generation", secondFileName)
    }

    "list a folder in existing bucket using multiple requests" in {
      val firstFileName = "file1.txt"
      val secondFileName = "file2.txt"
      val folder = "folder"
      val versions = true

      mock.simulate(
        mockTokenApi,
        mockBucketListing(firstFileName, secondFileName, Some(folder)),
        mockBucketListing(firstFileName, secondFileName, Some(folder), versions)
      )

      //#list-bucket

      val listSource: Source[StorageObject, NotUsed] = GCStorage.listBucket(bucketName, Some(folder))
      val listVersionsSource: Source[StorageObject, NotUsed] = GCStorage.listBucket(bucketName, Some(folder), versions)

      //#list-bucket

      listSource.runWith(Sink.seq).futureValue.map(_.name) shouldBe Seq(firstFileName, secondFileName)
      listVersionsSource.runWith(Sink.seq).futureValue.map(_.name) shouldBe
      Seq(firstFileName, s"$firstFileName#$generation", secondFileName)
    }

    "fail with error when bucket listing fails" in {

      mock.simulate(
        mockTokenApi,
        mockBucketListingFailure()
      )

      val listSource = GCStorage.listBucket(bucketName, None)

      listSource.runWith(Sink.seq).failed.futureValue.getMessage shouldBe "[400] Bucket listing failed"
    }

    // TODO Is this really the desired behavior?
    "return empty source listing bucket with wrong settings" ignore {

      mock.simulate(
        mockTokenApi,
        mockBucketListingFailure()
      )

      //#list-bucket-attributes

      val newSettings = GoogleSettings(system).withProjectId("projectId")

      val listSource: Source[StorageObject, NotUsed] =
        GCStorage.listBucket(bucketName, None).withAttributes(GoogleAttributes.settings(newSettings))

      //#list-bucket-attributes

      listSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "get existing storage object" in {

      mock.simulate(
        mockTokenApi,
        mockGetExistingStorageObject(),
        mockGetExistingStorageObject(Some(generation))
      )

      //#objectMetadata

      val getObjectSource: Source[Option[StorageObject], NotUsed] = GCStorage.getObject(bucketName, fileName)

      val getObjectGenerationSource: Source[Option[StorageObject], NotUsed] =
        GCStorage.getObject(bucketName, fileName, Some(generation))

      //#objectMetadata

      val result = getObjectSource.runWith(Sink.head).futureValue
      val resultGeneration = getObjectGenerationSource.runWith(Sink.head).futureValue

      result.map(_.name) shouldBe Some(fileName)
      result.map(_.bucket) shouldBe Some(bucketName)

      resultGeneration.map(_.name) shouldBe Some(fileName)
      resultGeneration.map(_.bucket) shouldBe Some(bucketName)
      resultGeneration.map(_.generation) shouldBe Some(generation)
    }

    "get None if storage object doesn't exist" in {

      mock.simulate(
        mockTokenApi,
        mockGetNonExistingStorageObject()
      )

      val getObjectSource = GCStorage.getObject(bucketName, fileName)

      getObjectSource.runWith(Sink.head).futureValue shouldBe None
    }

    "fail with error when get storage object fails" in {

      mock.simulate(
        mockTokenApi,
        mockGetNonStorageObjectFailure()
      )

      val getObjectSource = GCStorage.getObject(bucketName, fileName)

      getObjectSource.runWith(Sink.head).failed.futureValue.getMessage shouldBe "[400] Get storage object failed"
    }

    "download file when file exists" in {
      // Ensure file content is above the size limit set by akka.http.client.parsing.max-content-length
      val fileContent = "Google storage file content" + ("x" * 10000)
      val fileContentGeneration = "Google storage file content (archived)"

      mock.simulate(
        mockTokenApi,
        mockFileDownload(fileContent),
        mockFileDownload(fileContentGeneration, Some(generation))
      )

      //#download

      val downloadSource: Source[Option[Source[ByteString, NotUsed]], NotUsed] =
        GCStorage.download(bucketName, fileName)

      val downloadGenerationSource: Source[Option[Source[ByteString, NotUsed]], NotUsed] =
        GCStorage.download(bucketName, fileName, Some(generation))

      //#download

      val data: Future[Option[Source[ByteString, NotUsed]]] =
        downloadSource.runWith(Sink.head)

      val dataGeneration: Future[Option[Source[ByteString, NotUsed]]] =
        downloadGenerationSource.runWith(Sink.head)

      import system.dispatcher

      val result: Future[Seq[String]] = data
        .flatMap(_.getOrElse(Source.empty).map(_.utf8String).runWith(Sink.seq[String]))

      val resultGeneration: Future[Seq[String]] = dataGeneration
        .flatMap(_.getOrElse(Source.empty).map(_.utf8String).runWith(Sink.seq[String]))

      result.futureValue.mkString shouldBe fileContent
      resultGeneration.futureValue.mkString shouldBe fileContentGeneration

    }

    "download results in None when file doesn't exist" in {
      val bucketName = "alpakka"
      val fileName = "file1.txt"

      mock.simulate(
        mockTokenApi,
        mockNonExistingFileDownload()
      )

      val downloadSource = GCStorage.download(bucketName, fileName)

      downloadSource.runWith(Sink.head).futureValue shouldBe None
    }

    "fail with error when file download fails" in {
      val bucketName = "alpakka"
      val fileName = "file1.txt"

      mock.simulate(
        mockTokenApi,
        mockFileDownloadFailure()
      )

      val downloadSource = GCStorage.download(bucketName, fileName)
      downloadSource
        .runWith(Sink.head)
        .failed
        .futureValue
        .getMessage shouldBe "[400] File download failed"
    }

    "automatically retry when file downloads fail" in {
      val bucketName = "alpakka"
      val fileName = "file1.txt"
      val fileContent = "This is the file content"

      mock.simulate(
        mockTokenApi,
        mockFileDownloadFailureThenSuccess(500, "Internal server error", fileContent)
      )

      val downloadSource = GCStorage.download(bucketName, fileName)

      val data: Future[Option[Source[ByteString, NotUsed]]] =
        downloadSource.runWith(Sink.head)

      import system.dispatcher

      val result: Future[Seq[String]] = data
        .flatMap(_.getOrElse(Source.empty).map(_.utf8String).runWith(Sink.seq[String]))

      result.futureValue.mkString shouldBe fileContent
    }

    // retry for all akka-http standard server errors (5XX) https://github.com/akka/alpakka/issues/2057
    "automatically retry when file downloads fail with a non-500 server error" in {
      val bucketName = "alpakka"
      val fileName = "file1.txt"
      val fileContent = "This is the file content"

      mock.simulate(
        mockTokenApi,
        mockFileDownloadFailureThenSuccess(503, "Backend Error", fileContent)
      )

      val downloadSource = GCStorage.download(bucketName, fileName)

      val data: Future[Option[Source[ByteString, NotUsed]]] =
        downloadSource.runWith(Sink.head)

      import system.dispatcher

      val result: Future[Seq[String]] = data
        .flatMap(_.getOrElse(Source.empty).map(_.utf8String).runWith(Sink.seq[String]))

      result.futureValue.mkString shouldBe fileContent
    }

    "upload small file" in {
      val fileContent = "chunk1"
      val contentType = ContentTypes.`application/octet-stream`
      val fileSource = Source.single(ByteString(fileContent))

      mock.simulate(
        mockTokenApi,
        mockUploadSmallFile(fileContent)
      )

      val simpleUploadSource = GCStorage.simpleUpload(bucketName, fileName, fileSource, contentType)

      val storageObject = simpleUploadSource.runWith(Sink.head).futureValue

      storageObject.name shouldBe fileName
      storageObject.bucket shouldBe bucketName
    }

    "fail with error when small file upload fails" in {
      val fileContent = "chunk1"
      val contentType = ContentTypes.`application/octet-stream`
      val fileSource = Source.single(ByteString(fileContent))

      mock.simulate(
        mockTokenApi,
        mockUploadSmallFileFailure(fileContent)
      )

      val simpleUploadSource = GCStorage.simpleUpload(bucketName, fileName, fileSource, contentType)

      simpleUploadSource.runWith(Sink.head).failed.futureValue.getMessage shouldBe "[400] Upload small file failed"
    }

    "delete existing object" in {

      mock.simulate(
        mockTokenApi,
        mockDeleteObject(fileName),
        mockDeleteObject(fileName, Some(generation))
      )

      val deleteSource = GCStorage.deleteObject(bucketName, fileName)
      val deleteGenerationSource = GCStorage.deleteObject(bucketName, fileName, Some(generation))

      deleteSource.runWith(Sink.head).futureValue shouldBe true
      deleteGenerationSource.runWith(Sink.head).futureValue shouldBe true
    }

    "not delete non existing object" in {

      mock.simulate(
        mockTokenApi,
        mockNonExistingDeleteObject(fileName)
      )

      val deleteSource = GCStorage.deleteObject(bucketName, fileName)
      deleteSource.runWith(Sink.head).futureValue shouldBe false
    }

    "fail when delete object fails" in {

      mock.simulate(
        mockTokenApi,
        mockDeleteObjectFailure(fileName)
      )

      val deleteSource = GCStorage.deleteObject(bucketName, fileName)

      deleteSource.runWith(Sink.ignore).failed.futureValue.getMessage shouldBe "[400] Delete object failed"
    }

    "delete existing folder" in {
      val firstFileName = "file1.txt"
      val secondFileName = "file2.txt"
      val prefix = "folder"

      mock.simulate(
        mockTokenApi,
        mockBucketListing(firstFileName, secondFileName, Some(prefix)),
        mockDeleteObject(firstFileName),
        mockDeleteObject(secondFileName)
      )

      val deleteObjectsByPrefixSource = GCStorage.deleteObjectsByPrefix(bucketName, Some(prefix))
      deleteObjectsByPrefixSource.runWith(Sink.seq).futureValue shouldBe Seq(true, true)
    }

    "not delete non existing folder" in {
      val prefix = "folder"

      mock.simulate(
        mockTokenApi,
        mockNonExistingBucketListing(Some(prefix)),
        mockObjectDoesNotExist(prefix)
      )

      val deleteObjectsByPrefixSource = GCStorage.deleteObjectsByPrefix(bucketName, Some(prefix))

      deleteObjectsByPrefixSource.runWith(Sink.seq).futureValue shouldBe empty
    }

    "fail when folder delete fails" in {
      val firstFileName = "file1.txt"
      val secondFileName = "file2.txt"
      val prefix = "folder"

      mock.simulate(
        mockTokenApi,
        mockBucketListing(firstFileName, secondFileName, Some(prefix)),
        mockDeleteObject(firstFileName),
        mockDeleteObjectFailure(secondFileName)
      )

      val deleteObjectsByPrefixSource = GCStorage.deleteObjectsByPrefix(bucketName, Some(prefix))
      deleteObjectsByPrefixSource.runWith(Sink.seq).failed.futureValue.getMessage shouldBe "[400] Delete object failed"
    }
  }
}

object TestCredentials {
  val accessToken =
    "ya29.Elz4A2XkfGKJ4CoS5x_umUBHsvjGdeWQzu6gRRCnNXI0fuIyoDP_6aYktBQEOI4YAhLNgUl2OpxWQaN8Z3hd5YfFw1y4EGAtr2o28vSID-c8ul_xxHuudE7RmhH9sg"
}
