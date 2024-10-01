/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.stream.{Attributes, KillSwitches, SharedKillSwitch}
import akka.stream.alpakka.s3.AccessStyle.{PathAccessStyle, VirtualHostAccessStyle}
import akka.stream.alpakka.s3.BucketAccess.{AccessGranted, NotExists}
import akka.stream.alpakka.s3._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.{TestKit, TestKitBase}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.scalatest.Inspectors.forEvery
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers._

import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

trait S3IntegrationSpec
    extends AnyFlatSpecLike
    with TestKitBase
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with OptionValues
    with LogCapturing {

  implicit val ec: ExecutionContext = system.dispatcher

  implicit val defaultPatience: PatienceConfig = PatienceConfig(3.minutes, 100.millis)

  val defaultBucket = "my-test-us-east-1"
  val nonExistingBucket = "nowhere"

  // with dots forcing path style access
  val bucketWithDots = "my.test.frankfurt"

  val bucketWithVersioning = "my-bucket-with-versioning"

  val objectKey = "test"

  val objectValue = "Some String"
  val metaHeaders: Map[String, String] = Map("location" -> "Africa", "datatype" -> "image")

  override protected def afterAll(): Unit =
    Http(system)
      .shutdownAllConnectionPools()
      .foreach(_ => TestKit.shutdownActorSystem(system))

  lazy val defaultS3Settings: S3Settings =
    S3Settings()
      .withS3RegionProvider(
        new AwsRegionProvider {
          val getRegion: Region = Region.US_EAST_1
        }
      )
      .withAccessStyle(PathAccessStyle)

  def attributes: Attributes =
    S3Attributes.settings(defaultS3Settings)

  def attributes(block: S3Settings => S3Settings): Attributes =
    S3Attributes.settings(
      block(defaultS3Settings)
    )

  def otherRegionSettingsPathStyleAccess =
    attributes(
      _.withAccessStyle(PathAccessStyle)
        .withS3RegionProvider(new AwsRegionProvider {
          val getRegion: Region = Region.EU_CENTRAL_1
        })
    )

  def invalidCredentials: Attributes =
    attributes(
      _.withCredentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("invalid", "invalid"))
      )
    )

  def defaultRegionContentCount = 4
  def otherRegionContentCount = 5

  it should "list buckets in current aws account" in {
    val result = for {
      buckets <- S3.listBuckets().withAttributes(attributes).runWith(Sink.seq)
    } yield buckets

    val buckets = result.futureValue

    buckets.map(_.name) should contain(defaultBucket)
  }

  it should "list with real credentials" in {
    val result = S3
      .listBucket(defaultBucket, None)
      .withAttributes(attributes)
      .runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe defaultRegionContentCount
  }

  it should "list with real credentials using the Version 1 API" in {
    val result = S3
      .listBucket(defaultBucket, None)
      .withAttributes(attributes(_.withListBucketApiVersion(ApiVersion.ListBucketVersion1)))
      .runWith(Sink.seq)

    val listingResult = result.futureValue
    listingResult.size shouldBe defaultRegionContentCount
  }

  it should "list with real credentials in non us-east-1 zone" in {
    val result = S3
      .listBucket(bucketWithDots, None)
      .withAttributes(otherRegionSettingsPathStyleAccess)
      .runWith(Sink.seq)

    result.futureValue.size shouldBe otherRegionContentCount
  }

  it should "upload with real credentials" in {
    val objectKey = "putTest"
    val bytes = ByteString(objectValue)
    val data = Source.single(ByteString(objectValue))

    val result =
      S3.putObject(defaultBucket,
                   objectKey,
                   data,
                   bytes.length,
                   s3Headers = S3Headers().withMetaHeaders(MetaHeaders(metaHeaders)))
        .withAttributes(attributes)
        .runWith(Sink.head)

    result.futureValue.eTag should not be empty
  }

  it should "upload and delete" in {
    val objectKey = "putTest"
    val bytes = ByteString(objectValue)
    val data = Source.single(ByteString(objectValue))

    val result = for {
      put <- S3
        .putObject(defaultBucket,
                   objectKey,
                   data,
                   bytes.length,
                   s3Headers = S3Headers().withMetaHeaders(MetaHeaders(metaHeaders)))
        .withAttributes(attributes)
        .runWith(Sink.head)
      metaBefore <- S3.getObjectMetadata(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head)
      delete <- S3.deleteObject(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head)
      metaAfter <- S3.getObjectMetadata(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head)
    } yield {
      (put, delete, metaBefore, metaAfter)
    }

    val (putResult, _, metaBefore, metaAfter) = result.futureValue
    putResult.eTag should not be empty
    metaBefore should not be empty
    metaBefore.get.contentType shouldBe Some(ContentTypes.`application/octet-stream`.value)
    metaAfter shouldBe empty
  }

  it should "upload multipart with real credentials" in {
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val result =
      source
        .runWith(
          S3.multipartUpload(defaultBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(attributes)
        )

    val multipartUploadResult = result.futureValue
    multipartUploadResult.bucket shouldBe defaultBucket
    multipartUploadResult.key shouldBe objectKey
  }

  it should "download with real credentials" in {
    val (metaFuture, bodyFuture) = S3
      .getObject(defaultBucket, objectKey)
      .withAttributes(attributes)
      .map(_.decodeString("utf8"))
      .toMat(Sink.head)(Keep.both)
      .run()

    bodyFuture.futureValue shouldBe objectValue
    val meta = metaFuture.futureValue
    meta.eTag should not be empty
    meta.contentType shouldBe Some(ContentTypes.`application/octet-stream`.value)
  }

  it should "delete with real credentials" in {
    val delete = S3
      .deleteObject(defaultBucket, objectKey)
      .withAttributes(attributes)
      .runWith(Sink.head)
    delete.futureValue shouldEqual akka.Done
  }

  it should "upload huge multipart with real credentials" in {
    val objectKey = "huge"
    val hugeString = "0123456789abcdef" * 64 * 1024 * 11
    val result =
      Source
        .single(ByteString(hugeString))
        .runWith(
          S3.multipartUpload(defaultBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(attributes)
        )

    val multipartUploadResult = result.futureValue
    multipartUploadResult.bucket shouldBe defaultBucket
    multipartUploadResult.key shouldBe objectKey
  }

  it should "upload, download and delete with spaces in the key" in {
    val objectKey = "test folder/test file.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(defaultBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(attributes)
        )
      download <- S3
        .getObject(defaultBucket, objectKey)
        .withAttributes(attributes)
        .map(_.decodeString("utf8"))
        .runWith(Sink.head)
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = results.futureValue

    multipartUploadResult.bucket shouldBe defaultBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(defaultBucket, objectKey)
      .withAttributes(attributes)
      .runWith(Sink.head)
      .futureValue shouldEqual akka.Done
  }

  it should "upload, download and delete with brackets in the key" in {
    val objectKey = "abc/DEF/2017/06/15/1234 (1).TXT"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(defaultBucket, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(attributes)
        )
      download <- S3
        .getObject(defaultBucket, objectKey)
        .withAttributes(attributes)
        .map(_.decodeString("utf8"))
        .runWith(Sink.head)
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = results.futureValue

    multipartUploadResult.bucket shouldBe defaultBucket
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(defaultBucket, objectKey)
      .withAttributes(attributes)
      .runWith(Sink.head)
      .futureValue shouldEqual akka.Done
  }

  it should "upload, download and delete with spaces in the key in non us-east-1 zone" in uploadDownloadAndDeleteInOtherRegionCase(
    "test folder/test file.txt"
  )

  // we want ASCII and other UTF-8 characters!
  it should "upload, download and delete with special characters in the key in non us-east-1 zone" in uploadDownloadAndDeleteInOtherRegionCase(
    "føldęrü/1234()[]><!? .TXT"
  )

  it should "upload, download and delete with `+` character in the key in non us-east-1 zone" in uploadDownloadAndDeleteInOtherRegionCase(
    "1 + 2 = 3"
  )

  it should "upload, copy, download the copy, and delete" in uploadCopyDownload(
    "original/file.txt",
    "copy/file.txt"
  )

  // NOTE: MinIO currently has problems copying files with spaces.
  it should "upload, copy, download the copy, and delete with special characters in key" in uploadCopyDownload(
    "original/føldęrü/1234()[]><!?.TXT",
    "copy/1 + 2 = 3"
  )

  it should "upload 2 files with common prefix, 1 with different prefix and delete by prefix" in {
    val sourceKey1 = "original/file1.txt"
    val sourceKey2 = "original/file2.txt"
    val sourceKey3 = "uploaded/file3.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload1 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey1).withAttributes(attributes))
      upload2 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey2).withAttributes(attributes))
      upload3 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey3).withAttributes(attributes))
    } yield (upload1, upload2, upload3)

    whenReady(results) {
      case (upload1, upload2, upload3) =>
        upload1.bucket shouldEqual defaultBucket
        upload1.key shouldEqual sourceKey1
        upload2.bucket shouldEqual defaultBucket
        upload2.key shouldEqual sourceKey2
        upload3.bucket shouldEqual defaultBucket
        upload3.key shouldEqual sourceKey3

        S3.deleteObjectsByPrefix(defaultBucket, Some("original"))
          .withAttributes(attributes)
          .runWith(Sink.ignore)
          .futureValue shouldEqual akka.Done
        val numOfKeysForPrefix =
          S3.listBucket(defaultBucket, Some("original"))
            .withAttributes(attributes)
            .runFold(0)((result, _) => result + 1)
            .futureValue
        numOfKeysForPrefix shouldEqual 0
        S3.deleteObject(defaultBucket, sourceKey3)
          .withAttributes(attributes)
          .runWith(Sink.head)
          .futureValue shouldEqual akka.Done
    }
  }

  it should "create multiple versions of an object and successfully clean it with deleteBucketContents" in {
    // TODO: Figure out a way to properly test this with Minio, see https://github.com/akka/alpakka/issues/2750
    assume(this.isInstanceOf[AWSS3IntegrationSpec])
    val versionKey = "test-version"
    val one = ByteString("one")
    val two = ByteString("two")
    val three = ByteString("three")

    val results = for {
      // Clean the bucket just incase there is residual data in there
      _ <- S3
        .deleteBucketContents(bucketWithVersioning, deleteAllVersions = true)
        .withAttributes(attributes)
        .runWith(Sink.ignore)
      _ <- S3
        .putObject(bucketWithVersioning, versionKey, Source.single(one), one.length, s3Headers = S3Headers())
        .withAttributes(attributes)
        .runWith(Sink.ignore)
      _ <- S3
        .putObject(bucketWithVersioning, versionKey, Source.single(two), two.length, s3Headers = S3Headers())
        .withAttributes(attributes)
        .runWith(Sink.ignore)
      _ <- S3
        .putObject(bucketWithVersioning, versionKey, Source.single(three), three.length, s3Headers = S3Headers())
        .withAttributes(attributes)
        .runWith(Sink.ignore)
      versionsBeforeDelete <- S3
        .listObjectVersions(bucketWithVersioning, None)
        .withAttributes(attributes)
        .runWith(Sink.seq)
      _ <- S3
        .deleteBucketContents(bucketWithVersioning, deleteAllVersions = true)
        .withAttributes(attributes)
        .runWith(Sink.ignore)
      versionsAfterDelete <- S3
        .listObjectVersions(bucketWithVersioning, None)
        .withAttributes(attributes)
        .runWith(Sink.seq)
      listBucketContentsAfterDelete <- S3
        .listBucket(bucketWithVersioning, None)
        .withAttributes(attributes)
        .runWith(Sink.seq)

    } yield (versionsBeforeDelete.flatMap { case (versions, _) => versions }, versionsAfterDelete.flatMap {
      case (versions, _) => versions
    }, listBucketContentsAfterDelete)

    val (versionsBeforeDelete, versionsAfterDelete, bucketContentsAfterDelete) = results.futureValue

    versionsBeforeDelete.size shouldEqual 3
    versionsAfterDelete.size shouldEqual 0
    bucketContentsAfterDelete.size shouldEqual 0
  }

  it should "listing object versions for a non versioned bucket should return None for versionId" in {
    // TODO: Figure out a way to properly test this with Minio, see https://github.com/akka/alpakka/issues/2750
    assume(this.isInstanceOf[AWSS3IntegrationSpec])
    val objectKey = "listObjectVersionIdTest"
    val bytes = ByteString(objectValue)
    val data = Source.single(ByteString(objectValue))
    val results = for {
      _ <- S3
        .putObject(defaultBucket,
                   objectKey,
                   data,
                   bytes.length,
                   s3Headers = S3Headers().withMetaHeaders(MetaHeaders(metaHeaders)))
        .withAttributes(attributes)
        .runWith(Sink.ignore)
      result <- S3.listObjectVersions(defaultBucket, None).withAttributes(attributes).runWith(Sink.seq)
      _ <- S3.deleteObject(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.ignore)
    } yield result.flatMap { case (versions, _) => versions }

    forEvery(results.futureValue) { version =>
      version.versionId shouldEqual None
    }
  }

  it should "upload 2 files, delete all files in bucket" in {
    val sourceKey1 = "original/file1.txt"
    val sourceKey2 = "original/file2.txt"
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload1 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey1).withAttributes(attributes))
      upload2 <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey2).withAttributes(attributes))
    } yield (upload1, upload2)

    whenReady(results) {
      case (upload1, upload2) =>
        upload1.bucket shouldEqual defaultBucket
        upload1.key shouldEqual sourceKey1
        upload2.bucket shouldEqual defaultBucket
        upload2.key shouldEqual sourceKey2

        S3.deleteObjectsByPrefix(defaultBucket, prefix = None)
          .withAttributes(attributes)
          .runWith(Sink.ignore)
          .futureValue shouldEqual akka.Done
        val numOfKeysForPrefix =
          S3.listBucket(defaultBucket, None)
            .withAttributes(attributes)
            .runFold(0)((result, _) => result + 1)
            .futureValue
        numOfKeysForPrefix shouldEqual 0
    }
  }

  @tailrec
  final def createStringCollectionWithMinChunkSizeRec(numberOfChunks: Int,
                                                      stringAcc: BigInt = BigInt(0),
                                                      currentChunk: Int = 0,
                                                      result: Vector[ByteString] = Vector.empty): Vector[ByteString] = {

    if (currentChunk == numberOfChunks)
      result
    else {
      val newAcc = stringAcc + 1

      result.lift(currentChunk) match {
        case Some(currentString) =>
          val newString = ByteString(s"\n${newAcc.toString()}")
          //TODO: Performance here can be improved by using the same technique as in
          //createStringCollectionContextWithMinChunkSizeRec to keep track of the current string size in bytes
          if (currentString.toArray.length < S3.MinChunkSize) {
            val appendedString = currentString ++ newString
            createStringCollectionWithMinChunkSizeRec(numberOfChunks,
                                                      newAcc,
                                                      currentChunk,
                                                      result.updated(currentChunk, appendedString))
          } else {
            val newChunk = currentChunk + 1
            val newResult = {
              // // We are at the last index at this point so don't append a new entry at the end of the Vector
              if (currentChunk == numberOfChunks - 1)
                result
              else
                result :+ newString
            }
            createStringCollectionWithMinChunkSizeRec(numberOfChunks, newAcc, newChunk, newResult)
          }
        case None =>
          // This case happens right at the start
          val firstResult = Vector(ByteString("1"))
          createStringCollectionWithMinChunkSizeRec(numberOfChunks, newAcc, currentChunk, firstResult)
      }
    }
  }

  /**
   * Creates a `List` of `ByteString` where the size of each ByteString is guaranteed to be at least `S3.MinChunkSize`
   * in size.
   *
   * This is useful for tests that deal with multipart uploads, since S3 persists a part everytime it receives
   * `S3.MinChunkSize` bytes
   * @param numberOfChunks The number of chunks to create
   * @return A List of `ByteString` where each element is at least `S3.MinChunkSize` in size
   */
  def createStringCollectionWithMinChunkSize(numberOfChunks: Int): List[ByteString] =
    createStringCollectionWithMinChunkSizeRec(numberOfChunks).toList

  case object AbortException extends Exception("Aborting multipart upload")

  def createSlowSource(data: immutable.Seq[ByteString],
                       killSwitch: Option[SharedKillSwitch]): Source[ByteString, NotUsed] = {
    val base = Source(data)
      .throttle(1, 10.seconds)

    killSwitch.fold(base)(ks => base.viaMat(ks.flow)(Keep.left))
  }

  def byteStringToMD5(byteString: ByteString): String = {
    import java.math.BigInteger
    import java.security.MessageDigest

    val digest = MessageDigest.getInstance("MD5")
    digest.update(byteString.asByteBuffer)
    String.format("%032X", new BigInteger(1, digest.digest())).toLowerCase
  }

  it should "upload 1 file slowly, cancel it and retrieve a multipart upload + list part" in {
    // This test doesn't work on Minio since minio doesn't properly implement this API, see
    // https://github.com/minio/minio/issues/13246
    assume(this.isInstanceOf[AWSS3IntegrationSpec])
    val sourceKey = "original/file-slow.txt"
    val sharedKillSwitch = KillSwitches.shared("abort-multipart-upload")

    val inputData = createStringCollectionWithMinChunkSize(5)
    val slowSource = createSlowSource(inputData, Some(sharedKillSwitch))

    val multiPartUpload =
      slowSource.toMat(S3.multipartUpload(defaultBucket, sourceKey).withAttributes(attributes))(Keep.right).run()

    val results = for {
      _ <- akka.pattern.after(25.seconds)(Future {
        sharedKillSwitch.abort(AbortException)
      })
      _ <- multiPartUpload.recover[Any] {
        case AbortException => ()
      }
      incomplete <- S3.listMultipartUpload(defaultBucket, None).withAttributes(attributes).runWith(Sink.seq)
      uploadIds = incomplete.collect {
        case uploadPart if uploadPart.key == sourceKey => uploadPart.uploadId
      }
      parts <- Future.sequence(uploadIds.map { uploadId =>
        S3.listParts(defaultBucket, sourceKey, uploadId).runWith(Sink.seq)
      })
      // Cleanup the uploads after
      _ <- Future.sequence(uploadIds.map { uploadId =>
        S3.deleteUpload(defaultBucket, sourceKey, uploadId)
      })
    } yield (uploadIds, incomplete, parts.flatten)

    whenReady(results) {
      case (uploadIds, incompleteFiles, parts) =>
        val inputsUntilAbort = inputData.slice(0, 3)
        incompleteFiles.exists(_.key == sourceKey) shouldBe true
        parts.nonEmpty shouldBe true
        uploadIds.size shouldBe 1
        parts.size shouldBe 3
        parts.map(_.size) shouldBe inputsUntilAbort.map(_.utf8String.getBytes("UTF-8").length)
        // In S3 the etag's are actually an MD5 hash of the contents of the part so we can use this to check
        // that the data has been uploaded correctly and in the right order, see
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
        parts.map(_.eTag.replaceAll("\"", "")) shouldBe inputsUntilAbort.map(byteStringToMD5)
    }
  }

  it should "upload 1 file slowly, cancel it and then resume it to complete the upload" in {
    // This test doesn't work on Minio since minio doesn't properly implement this API, see
    // https://github.com/minio/minio/issues/13246
    assume(this.isInstanceOf[AWSS3IntegrationSpec])
    val sourceKey = "original/file-slow-2.txt"
    val sharedKillSwitch = KillSwitches.shared("abort-multipart-upload-2")

    val inputData = createStringCollectionWithMinChunkSize(6)

    val slowSource = createSlowSource(inputData, Some(sharedKillSwitch))

    val multiPartUpload =
      slowSource
        .toMat(S3.multipartUpload(defaultBucket, sourceKey).withAttributes(attributes))(
          Keep.right
        )
        .run()

    val results = for {
      _ <- akka.pattern.after(25.seconds)(Future {
        sharedKillSwitch.abort(AbortException)
      })
      _ <- multiPartUpload.recover[Any] {
        case AbortException => ()
      }
      incomplete <- S3.listMultipartUpload(defaultBucket, None).withAttributes(attributes).runWith(Sink.seq)

      uploadId = incomplete.collectFirst {
        case uploadPart if uploadPart.key == sourceKey => uploadPart.uploadId
      }.get

      parts <- S3.listParts(defaultBucket, sourceKey, uploadId).runWith(Sink.seq)

      remainingData = inputData.slice(3, 6)
      _ <- Source(remainingData)
        .toMat(
          S3.resumeMultipartUpload(defaultBucket, sourceKey, uploadId, parts.map(_.toPart))
            .withAttributes(attributes)
        )(
          Keep.right
        )
        .run()

      // This delay is here because sometimes there is a delay when you complete a large file and its
      // actually downloadable
      downloaded <- akka.pattern.after(5.seconds)(
        S3.getObject(defaultBucket, sourceKey).withAttributes(attributes).runWith(Sink.seq)
      )

      _ <- S3.deleteObject(defaultBucket, sourceKey).withAttributes(attributes).runWith(Sink.head)
    } yield downloaded

    whenReady(results) { downloads =>
      val fullDownloadedFile = downloads.fold(ByteString.empty)(_ ++ _)
      val fullInputData = inputData.fold(ByteString.empty)(_ ++ _)

      fullInputData.utf8String shouldEqual fullDownloadedFile.utf8String
    }
  }

  it should "upload a full file but complete it manually with S3.completeMultipartUploadSource" in {
    assume(this.isInstanceOf[AWSS3IntegrationSpec])
    val sourceKey = "original/file-slow-3.txt"
    val sharedKillSwitch = KillSwitches.shared("abort-multipart-upload-3")

    val inputData = createStringCollectionWithMinChunkSize(4)

    val slowSource = createSlowSource(inputData, Some(sharedKillSwitch))

    val multiPartUpload =
      slowSource
        .toMat(S3.multipartUpload(defaultBucket, sourceKey).withAttributes(attributes))(
          Keep.right
        )
        .run()

    val results = for {
      _ <- akka.pattern.after(25.seconds)(Future {
        sharedKillSwitch.abort(AbortException)
      })
      _ <- multiPartUpload.recover[Any] {
        case AbortException => ()
      }
      incomplete <- S3.listMultipartUpload(defaultBucket, None).withAttributes(attributes).runWith(Sink.seq)

      uploadId = incomplete.collectFirst {
        case uploadPart if uploadPart.key == sourceKey => uploadPart.uploadId
      }.get

      parts <- S3.listParts(defaultBucket, sourceKey, uploadId).runWith(Sink.seq)

      _ <- S3.completeMultipartUpload(defaultBucket, sourceKey, uploadId, parts.map(_.toPart))
      // This delay is here because sometimes there is a delay when you complete a large file and its
      // actually downloadable
      downloaded <- akka.pattern.after(5.seconds)(
        S3.getObject(defaultBucket, sourceKey).withAttributes(attributes).runWith(Sink.seq)
      )
      _ <- S3.deleteObject(defaultBucket, sourceKey).withAttributes(attributes).runWith(Sink.head)
    } yield downloaded

    whenReady(results) { downloads =>
      val fullDownloadedFile = downloads.fold(ByteString.empty)(_ ++ _)
      val fullInputData = inputData.slice(0, 3).fold(ByteString.empty)(_ ++ _)

      fullInputData.utf8String shouldEqual fullDownloadedFile.utf8String
    }
  }
  @tailrec
  final def createStringCollectionContextWithMinChunkSizeRec(
      numberOfChunks: Int,
      stringAcc: BigInt = BigInt(0),
      currentChunk: Int = 0,
      currentChunkSize: Int = 0,
      result: Vector[Vector[(ByteString, BigInt)]] = Vector.empty
  ): Vector[Vector[(ByteString, BigInt)]] = {

    if (currentChunk == numberOfChunks)
      result
    else {
      val newAcc = stringAcc + 1

      result.lift(currentChunk) match {
        case Some(currentStrings) =>
          val newString = ByteString(s"\n${newAcc.toString()}")
          if (currentChunkSize < S3.MinChunkSize) {
            val newChunkSize = currentChunkSize + newString.toArray.length
            val newEntry = currentStrings :+ ((newString, newAcc))
            createStringCollectionContextWithMinChunkSizeRec(numberOfChunks,
                                                             newAcc,
                                                             currentChunk,
                                                             newChunkSize,
                                                             result.updated(currentChunk, newEntry))
          } else {
            val newChunk = currentChunk + 1
            val (newResult, newChunkSize) = {
              // // We are at the last index at this point so don't append a new entry at the end of the Vector
              if (currentChunk == numberOfChunks - 1)
                (result, currentChunkSize)
              else
                (result :+ Vector((newString, newAcc)), newString.toArray.length)
            }
            createStringCollectionContextWithMinChunkSizeRec(numberOfChunks, newAcc, newChunk, newChunkSize, newResult)
          }
        case None =>
          // This case happens right at the start
          val initial = ByteString("1")
          val firstResult = Vector(Vector((initial, newAcc)))
          createStringCollectionContextWithMinChunkSizeRec(numberOfChunks,
                                                           newAcc,
                                                           currentChunk,
                                                           initial.toArray.length,
                                                           firstResult)
      }
    }
  }

  /**
   * Creates a `List` of `List[(ByteString, BigInt)]` where the accumulated size of each ByteString in the list is
   * guaranteed to be at least `S3.MinChunkSize` in size.
   *
   * This is useful for tests that deal with multipart uploads, since S3 persists a part everytime it receives
   * `S3.MinChunkSize` bytes. Unlike `createStringCollectionWithMinChunkSizeRec` this version also adds an index
   * to each individual `ByteString` which is helpful when dealing with testing for context
   * @param numberOfChunks The number of chunks to create
   * @return A List of `List[ByteString]` where each element is at least `S3.MinChunkSize` in size along with an
   *         incrementing index for each `ByteString`
   */
  def createStringCollectionContextWithMinChunkSize(numberOfChunks: Int): List[List[(ByteString, BigInt)]] =
    createStringCollectionContextWithMinChunkSizeRec(numberOfChunks).toList.map(_.toList)

  it should "perform a chunked multi-part upload with the correct context" in {
    val sourceKey = "original/file-context-1.txt"
    val inputData = createStringCollectionContextWithMinChunkSize(4)
    val source = Source(inputData.flatten)
    val originalContexts = inputData.map(_.map { case (_, contexts) => contexts })
    val originalData = inputData.flatten.map { case (data, _) => data }.fold(ByteString.empty)(_ ++ _)
    val resultingContexts = new ConcurrentLinkedQueue[List[BigInt]]()
    val collectContextSink = Sink
      .foreach[(UploadPartResponse, immutable.Iterable[BigInt])] {
        case (_, contexts) =>
          resultingContexts.add(contexts.toList)
      }
      .mapMaterializedValue(_ => NotUsed)

    val results = for {
      _ <- source
        .toMat(
          S3.multipartUploadWithContext[BigInt](defaultBucket, sourceKey, collectContextSink, chunkingParallelism = 1)
            .withAttributes(attributes)
        )(
          Keep.right
        )
        .run()
      // This delay is here because sometimes there is a delay when you complete a large file and its
      // actually downloadable
      downloaded <- akka.pattern.after(5.seconds)(
        S3.getObject(defaultBucket, sourceKey).withAttributes(attributes).runWith(Sink.seq)
      )
      _ <- S3.deleteObject(defaultBucket, sourceKey).withAttributes(attributes).runWith(Sink.head)

    } yield downloaded

    whenReady(results) { downloads =>
      val fullDownloadedFile = downloads.fold(ByteString.empty)(_ ++ _)
      originalData.utf8String shouldEqual fullDownloadedFile.utf8String
      originalContexts shouldEqual resultingContexts.asScala.toList
    }
  }

  it should "make a bucket with given name" in {
    implicit val attr: Attributes = attributes
    val bucketName = "samplebucket1"

    val request: Future[Done] = S3
      .makeBucket(bucketName)

    whenReady(request) { value =>
      value shouldEqual Done

      S3.deleteBucket(bucketName).futureValue shouldBe Done
    }
  }

  it should "throw an exception while creating a bucket with the same name" in {
    implicit val attr: Attributes = attributes
    S3.makeBucket(defaultBucket).failed.futureValue shouldBe an[S3Exception]
  }

  it should "create and delete bucket with a given name" in {
    val bucketName = "samplebucket3"

    val makeRequest: Source[Done, NotUsed] = S3
      .makeBucketSource(bucketName)
      .withAttributes(attributes)
    val deleteRequest: Source[Done, NotUsed] = S3
      .deleteBucketSource(bucketName)
      .withAttributes(attributes)

    val request = for {
      make <- makeRequest.runWith(Sink.ignore)
      delete <- deleteRequest.runWith(Sink.ignore)
    } yield (make, delete)

    request.futureValue should equal((Done, Done))
  }

  it should "create a bucket in the non default us-east-1 region" in {
    val bucketName = "samplebucketotherregion"

    val request = for {
      _ <- S3
        .makeBucketSource(bucketName)
        .withAttributes(otherRegionSettingsPathStyleAccess)
        .runWith(Sink.head)
      result <- S3
        .checkIfBucketExistsSource(bucketName)
        .withAttributes(otherRegionSettingsPathStyleAccess)
        .runWith(Sink.head)
      _ <- S3
        .deleteBucketSource(bucketName)
        .withAttributes(otherRegionSettingsPathStyleAccess)
        .runWith(Sink.head)
    } yield result

    request.futureValue shouldEqual AccessGranted
  }

  it should "throw an exception while deleting bucket that doesn't exist" in {
    implicit val attr: Attributes = attributes
    S3.deleteBucket(nonExistingBucket).failed.futureValue shouldBe an[S3Exception]
  }

  it should "check if bucket exists" in {
    implicit val attr: Attributes = attributes
    val checkIfBucketExits: Future[BucketAccess] = S3.checkIfBucketExists(defaultBucket)

    whenReady(checkIfBucketExits) { bucketState =>
      bucketState should equal(AccessGranted)
    }
  }

  it should "check for non-existing bucket" in {
    implicit val attr: Attributes = attributes
    val request: Future[BucketAccess] = S3.checkIfBucketExists(nonExistingBucket)

    whenReady(request) { response =>
      response should equal(NotExists)
    }
  }

  it should "contain error code even if exception in empty" in {
    val exception =
      S3.getObjectMetadata(defaultBucket, "sample")
        .withAttributes(invalidCredentials)
        .runWith(Sink.head)
        .failed
        .mapTo[S3Exception]
        .futureValue

    exception.code shouldBe StatusCodes.Forbidden.toString()
  }

  private val chunk: ByteString = ByteString.fromArray(Array.fill(S3.MinChunkSize)(0.toByte))

  it should "only upload single chunk when size of the ByteString equals chunk size" in {
    val source: Source[ByteString, Any] = Source.single(chunk)
    uploadAndAndCheckParts(source, 1)
  }

  it should "only upload single chunk when exact chunk is followed by an empty ByteString" in {
    val source: Source[ByteString, Any] = Source[ByteString](
      chunk :: ByteString.empty :: Nil
    )

    uploadAndAndCheckParts(source, 1)
  }

  it should "upload two chunks size of ByteStrings equals chunk size" in {
    val source: Source[ByteString, Any] = Source(chunk :: chunk :: Nil)
    uploadAndAndCheckParts(source, 2)
  }

  it should "upload empty source" in {
    val upload =
      for {
        upload <- Source
          .empty[ByteString]
          .runWith(
            S3.multipartUpload(defaultBucket, objectKey, chunkSize = S3.MinChunkSize)
              .withAttributes(attributes)
          )
        _ <- S3.deleteObject(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head)
      } yield upload

    upload.futureValue.eTag should not be empty
  }

  private def uploadAndAndCheckParts(source: Source[ByteString, _], expectedParts: Int): Assertion = {
    val metadata =
      for {
        _ <- source.runWith(
          S3.multipartUpload(defaultBucket, objectKey, chunkSize = S3.MinChunkSize)
            .withAttributes(attributes)
        )
        metadata <- S3
          .getObjectMetadata(defaultBucket, objectKey)
          .withAttributes(attributes)
          .runWith(Sink.head)
        _ <- S3.deleteObject(defaultBucket, objectKey).withAttributes(attributes).runWith(Sink.head)
      } yield metadata

    val etag = metadata.futureValue.get.eTag.get
    etag.substring(etag.indexOf('-') + 1).toInt shouldBe expectedParts
  }

  private def uploadDownloadAndDeleteInOtherRegionCase(objectKey: String): Assertion = {
    val source: Source[ByteString, Any] = Source(ByteString(objectValue) :: Nil)

    val results = for {
      upload <- source
        .runWith(
          S3.multipartUpload(bucketWithDots, objectKey, metaHeaders = MetaHeaders(metaHeaders))
            .withAttributes(otherRegionSettingsPathStyleAccess)
        )
      download <- S3
        .getObject(bucketWithDots, objectKey)
        .withAttributes(otherRegionSettingsPathStyleAccess)
        .map(_.decodeString("utf8"))
        .runWith(Sink.head)
    } yield (upload, download)

    val (multipartUploadResult, downloaded) = results.futureValue

    multipartUploadResult.bucket shouldBe bucketWithDots
    multipartUploadResult.key shouldBe objectKey
    downloaded shouldBe objectValue

    S3.deleteObject(bucketWithDots, objectKey)
      .withAttributes(otherRegionSettingsPathStyleAccess)
      .runWith(Sink.head)
      .futureValue shouldEqual akka.Done
  }

  private def uploadCopyDownload(sourceKey: String, targetKey: String): Assertion = {
    val source: Source[ByteString, Any] = Source.single(ByteString(objectValue))

    val results = for {
      upload <- source.runWith(S3.multipartUpload(defaultBucket, sourceKey).withAttributes(attributes))
      copy <- S3
        .multipartCopy(defaultBucket, sourceKey, defaultBucket, targetKey)
        .withAttributes(attributes)
        .run()
      download <- S3
        .getObject(defaultBucket, targetKey)
        .withAttributes(attributes)
        .map(_.decodeString("utf8"))
        .runWith(Sink.head)
    } yield (upload, copy, download)

    whenReady(results) {
      case (upload, copy, downloaded) =>
        upload.bucket shouldEqual defaultBucket
        upload.key shouldEqual sourceKey
        copy.bucket shouldEqual defaultBucket
        copy.key shouldEqual targetKey
        downloaded shouldBe objectValue

        S3.deleteObject(defaultBucket, sourceKey)
          .withAttributes(attributes)
          .runWith(Sink.head)
          .futureValue shouldEqual akka.Done
        S3.deleteObject(defaultBucket, targetKey)
          .withAttributes(attributes)
          .runWith(Sink.head)
          .futureValue shouldEqual akka.Done
    }
  }
}

/*
 * This is an integration test and ignored by default
 *
 * For running the tests you need to create 2 buckets:
 *  - one in region us-east-1
 *  - one in an other region (eg eu-central-1)
 * Update the bucket name and regions in the code below
 *
 * Set your keys aws access-key-id and secret-access-key in src/test/resources/application.conf
 *
 * Comment @ignore and run the tests
 * (tests that do listing counts might need some tweaking)
 *
 */
@Ignore
class AWSS3IntegrationSpec extends TestKit(ActorSystem("AWSS3IntegrationSpec")) with S3IntegrationSpec

/*
 * For this test, you need a have docker installed and running.
 *
 * Run the tests from inside sbt:
 * s3/testOnly *.MinioS3IntegrationSpec
 */

class MinioS3IntegrationSpec
    extends TestKit(ActorSystem("MinioS3IntegrationSpec"))
    with S3IntegrationSpec
    with MinioS3Test {

  override val defaultRegionContentCount = 0
  override val otherRegionContentCount = 0

  override lazy val defaultS3Settings: S3Settings = s3Settings
    .withS3RegionProvider(
      new AwsRegionProvider {
        val getRegion: Region = Region.US_EAST_1
      }
    )
    .withAccessStyle(PathAccessStyle)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val makeBuckets = for {
      _ <- S3.makeBucket(bucketWithDots)(
        system,
        attributes(
          _.withS3RegionProvider(
            new AwsRegionProvider {
              val getRegion: Region = Region.EU_CENTRAL_1
            }
          )
        )
      )
      _ <- S3.makeBucket(defaultBucket)(
        system,
        attributes
      )
      _ <- S3.makeBucket(bucketWithVersioning)(
        system,
        attributes
      )
    } yield ()
    Await.result(makeBuckets, 10.seconds)
  }

  it should "properly set the endpointUrl using VirtualHostAccessStyle" in {
    s3Settings
      .withAccessStyle(VirtualHostAccessStyle)
      .withEndpointUrl(container.getVirtualHost)
      .endpointUrl
      .value shouldEqual container.getVirtualHost
  }
}
