/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.io._
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

import akka.actor.ActorSystem
import akka.stream.alpakka.file.{ArchiveMetadata, ArchiveMetadataWithSize}
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.testkit.TestKit
import akka.util.ByteString
import akka.{Done, NotUsed}
import docs.javadsl.ArchiveHelper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Success

class ArchiveSpec
    extends TestKit(ActorSystem("ArchiveSpec"))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing
    with IntegrationPatience {

  implicit val mat: Materializer = ActorMaterializer()

  private val archiveHelper = new ArchiveHelper()

  "archive" when {
    "zip flow is called" should {
      "pass empty stream" in {
        val sources = Source.empty
        val zipFlow = Archive.zip()

        val akkaZipped: Future[ByteString] =
          sources
            .via(zipFlow)
            .runWith(Sink.fold(ByteString.empty)(_ ++ _))

        akkaZipped.futureValue shouldBe ByteString.empty
      }

      "archive file" in {
        val filePath1 = getPathFromResources("akka_full_color.svg")
        val filePath2 = getPathFromResources("akka_icon_reverse.svg")
        val fileStream1: Source[ByteString, Any] = FileIO.fromPath(filePath1)
        val fileStream2: Source[ByteString, Any] = FileIO.fromPath(filePath2)

        /*
        // #sample-zip
        val fileStream1: Source[ByteString,  Any] = ...
        val fileStream2: Source[ByteString,  Any] = ...

        // #sample-zip
         */

        // #sample-zip
        val filesStream = Source(
          List(
            (ArchiveMetadata("akka_full_color.svg"), fileStream1),
            (ArchiveMetadata("akka_icon_reverse.svg"), fileStream2)
          )
        )

        val result = filesStream
          .via(Archive.zip())
          .runWith(FileIO.toPath(Paths.get("result.zip")))
        // #sample-zip
        result.futureValue shouldBe IOResult(1178, Success(Done))

        val resultFileContent =
          FileIO.fromPath(Paths.get("result.zip")).runWith(Sink.fold(ByteString.empty)(_ ++ _)).futureValue

        val unzipResultMap = archiveHelper.unzip(resultFileContent).asScala
        unzipResultMap should have size 2

        val refFile1 = FileIO
          .fromPath(filePath1)
          .runWith(Sink.fold(ByteString.empty)(_ ++ _))
          .futureValue
        val refFile2 = FileIO
          .fromPath(filePath2)
          .runWith(Sink.fold(ByteString.empty)(_ ++ _))
          .futureValue

        unzipResultMap("akka_full_color.svg") shouldBe refFile1
        unzipResultMap("akka_icon_reverse.svg") shouldBe refFile2

        //cleanup
        new File("result.zip").delete()
      }

      "archive files" in {
        val inputFiles = generateInputFiles(5, 100)
        val inputStream = filesToStream(inputFiles)
        val zipFlow = Archive.zip()

        val akkaZipped: Future[ByteString] =
          inputStream
            .via(zipFlow)
            .runWith(Sink.fold(ByteString.empty)(_ ++ _))

        archiveHelper.unzip(akkaZipped.futureValue).asScala shouldBe inputFiles
      }
    }

    "tar flow is called" should {
      "pass empty stream" in {
        val sources = Source.empty
        val tarFlow = Archive.tar()

        val akkaTarred: Future[ByteString] =
          sources
            .via(tarFlow)
            .runWith(Sink.fold(ByteString.empty)(_ ++ _))

        akkaTarred.futureValue shouldBe ByteString.empty
      }

      "archive file" in {
        val filePath1 = getPathFromResources("akka_full_color.svg")
        val filePath2 = getPathFromResources("akka_icon_reverse.svg")
        val fileStream1: Source[ByteString, Any] = FileIO.fromPath(filePath1)
        val fileStream2: Source[ByteString, Any] = FileIO.fromPath(filePath2)
        val fileContent1 = ByteString(Files.readAllBytes(filePath1))
        val fileContent2 = ByteString(Files.readAllBytes(filePath2))
        val fileSize1 = Files.size(filePath1)
        val fileSize2 = Files.size(filePath2)

        /*
        // #sample-tar
        val fileStream1: Source[ByteString,  Any] = ...
        val fileStream2: Source[ByteString,  Any] = ...
        val fileSize1: Long = ...
        val fileSize2: Long = ...

        // #sample-tar
         */

        // #sample-tar
        val filesStream = Source(
          List(
            (ArchiveMetadataWithSize("akka_full_color.svg", fileSize1), fileStream1),
            (ArchiveMetadataWithSize("akka_icon_reverse.svg", fileSize2), fileStream2)
          )
        )

        val result = filesStream
          .via(Archive.tar())
          .runWith(FileIO.toPath(Paths.get("result.tar")))
        // #sample-tar

        // #sample-tar-gz
        val resultGz = filesStream
          .via(Archive.tar().via(akka.stream.scaladsl.Compression.gzip))
          .runWith(FileIO.toPath(Paths.get("result.tar.gz")))
        // #sample-tar-gz

        result.futureValue shouldBe IOResult(3584, Success(Done))
        resultGz.futureValue.status shouldBe Success(Done)

        untar(Paths.get("result.tar").toRealPath(), "xf").foreach(
          _ shouldBe Map(
            "akka_full_color.svg" -> fileContent1,
            "akka_icon_reverse.svg" -> fileContent2
          )
        )
        untar(Paths.get("result.tar.gz").toRealPath(), "xfz").foreach(
          _ shouldBe Map(
            "akka_full_color.svg" -> fileContent1,
            "akka_icon_reverse.svg" -> fileContent2
          )
        )

        //cleanup
        new File("result.tar").delete()
        new File("result.tar.gz").delete()
      }

      "support long file paths" in {
        val fileName = "folder1-".padTo(40, 'x') + "/" + "folder2-".padTo(40, 'x') + "/" + "file-".padTo(80, 'x') + ".txt"
        val fileBytes = ByteString("test")

        val file = (ArchiveMetadataWithSize(fileName, fileBytes.size), Source.single(fileBytes))
        val filesStream = Source.single(file)
        val result = filesStream
          .via(Archive.tar())
          .runWith(FileIO.toPath(Paths.get("result.tar")))
        result.futureValue shouldBe IOResult(1024, Success(Done))

        untar(Paths.get("result.tar").toRealPath(), "xf").foreach(_ shouldBe Map(fileName -> fileBytes))

        //cleanup
        new File("result.tar").delete()
      }

      "fail if provided size and actual size do not match" in {
        val file = (ArchiveMetadataWithSize("file.txt", 10L), Source.single(ByteString("too long")))
        val filesStream = Source.single(file)
        val result = filesStream.via(Archive.tar()).runWith(Sink.ignore)
        an[IllegalStateException] should be thrownBy (throw result.failed.futureValue)
      }
    }
  }

  private def getPathFromResources(fileName: String): Path =
    Paths.get(getClass.getClassLoader.getResource(fileName).getPath)

  private def generateInputFiles(numberOfFiles: Int, lengthOfFile: Int): Map[String, Seq[Byte]] = {
    val r = new scala.util.Random(31)
    (1 to numberOfFiles)
      .map(number => s"file-$number" -> ByteString.fromArray(r.nextString(lengthOfFile).getBytes))
      .toMap
  }

  private def filesToStream(
      files: Map[String, Seq[Byte]]
  ): Source[(ArchiveMetadata, Source[ByteString, NotUsed]), NotUsed] = {
    val sourceFiles = files.toList.map {
      case (title, content) =>
        (ArchiveMetadata(title), Source(content.grouped(10).map(group => ByteString(group.toArray)).toList))
    }
    Source(sourceFiles)
  }

  // returns None if tar executable is not available on PATH
  private def untar(tarPath: Path, args: String): Option[Map[String, ByteString]] = {
    if (ExecutableUtils.isOnPath("tar")) {
      val tmpDir = Files.createTempDirectory("ArchiveSpec")
      try {
        ExecutableUtils.run("tar", Seq(args, tarPath.toString, "-C", tmpDir.toString), tmpDir).futureValue
        Some(
          Files
            .walk(tmpDir)
            .sorted(Comparator.reverseOrder())
            .iterator()
            .asScala
            .filter(path => Files.isRegularFile(path))
            .map(path => tmpDir.relativize(path).toString -> ByteString(Files.readAllBytes(path)))
            .toMap
        )
      } finally {
        Files.walk(tmpDir).sorted(Comparator.reverseOrder()).iterator().asScala.foreach(p => Files.delete(p))
      }
    } else None
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
