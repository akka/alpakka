/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.io._
import java.nio.file.{Files, Path, Paths}
import akka.actor.ActorSystem
import akka.stream.alpakka.file.ArchiveMetadata
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import akka.NotUsed
import docs.javadsl.ArchiveHelper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class ArchiveSpec
    extends TestKit(ActorSystem("ArchiveSpec"))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing
    with IntegrationPatience {

  implicit val ec: ExecutionContext = system.dispatcher

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
        result.futureValue.count shouldBe 1178

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

      "unarchive files" in {
        val inputFiles = generateInputFiles(5, 100)
        val inputStream = filesToStream(inputFiles)
        val zipFlow = Archive.zip()

        val akkaZipped: Future[ByteString] =
          inputStream
            .via(zipFlow)
            .runWith(Sink.fold(ByteString.empty)(_ ++ _))
        // #zip-reader
        val zipFile = // ???
          // #zip-reader
          File.createTempFile("pre", "post")
        zipFile.deleteOnExit()

        Source.future(akkaZipped).runWith(FileIO.toPath(zipFile.toPath)).futureValue

        Archive
          .zipReader(zipFile)
          .map(f => f._1.name -> f._2.runWith(Sink.fold(ByteString.empty)(_ ++ _)).futureValue)
          .runWith(Sink.seq)
          .futureValue
          .toMap shouldBe inputFiles

        // #zip-reader
        val target: Path = // ???
          // #zip-reader
          Files.createTempDirectory("alpakka-zip-")
        // #zip-reader
        Archive
          .zipReader(zipFile)
          .mapAsyncUnordered(4) {
            case (metadata, source) =>
              val targetFile = target.resolve(metadata.name)
              targetFile.toFile.getParentFile.mkdirs() //missing error handler
              source.runWith(FileIO.toPath(targetFile))
          }
        // #zip-reader
      }
    }
  }

  private def getPathFromResources(fileName: String): Path =
    Paths.get(getClass.getClassLoader.getResource(fileName).getPath)

  private def generateInputFiles(numberOfFiles: Int, lengthOfFile: Int): Map[String, ByteString] = {
    val r = new scala.util.Random(31)
    (1 to numberOfFiles)
      .map(number => s"file-$number" -> ByteString.fromArray(r.nextString(lengthOfFile).getBytes))
      .toMap
  }

  private def filesToStream(
      files: Map[String, ByteString]
  ): Source[(ArchiveMetadata, Source[ByteString, NotUsed]), NotUsed] = {
    val sourceFiles = files.toList.map {
      case (title, content) =>
        (ArchiveMetadata(title), Source(content.grouped(10).toList))
    }
    Source(sourceFiles)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
