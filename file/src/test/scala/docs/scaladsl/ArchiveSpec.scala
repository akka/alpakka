/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.io._
import java.nio.file.{Path, Paths}
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.file.ArchiveMetadata
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import akka.util.ByteString
import docs.javadsl.ArchiveHelper
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._
import scala.concurrent.Future
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ArchiveSpec
    extends TestKit(ActorSystem("ArchiveSpec"))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing {

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
         // #sample
         val fileStream1: Source[ByteString,  Any] = ...
         val fileStream2: Source[ByteString,  Any] = ...

         // #sample
         */

        // #sample
        val filesStream = Source(
          List(
            (ArchiveMetadata("akka_full_color.svg"), fileStream1),
            (ArchiveMetadata("akka_icon_reverse.svg"), fileStream2)
          )
        )

        val result = filesStream
          .via(Archive.zip())
          .runWith(FileIO.toPath(Paths.get("result.zip")))
        // #sample
        result.futureValue

        archiveHelper.createReferenceZipFile(List(filePath1, filePath2).asJava, "reference.zip")

        val resultFile = FileIO.fromPath(Paths.get("result.zip"))
        val referenceFile = FileIO.fromPath(Paths.get("reference.zip"))

        val resultFileContent = resultFile.runWith(Sink.fold(ByteString.empty)(_ ++ _)).futureValue
        val referenceFileContent = referenceFile.runWith(Sink.fold(ByteString.empty)(_ ++ _)).futureValue

        resultFileContent shouldBe referenceFileContent

        //cleanup
        new File("result.zip").delete()
        new File("reference.zip").delete()
      }

      "archive files" in {
        val inputFiles = generateInputFiles(5, 100)
        val inputStream = filesToStream(inputFiles)
        val zipFlow = Archive.zip()

        val akkaZipped: Future[ByteString] =
          inputStream
            .via(zipFlow)
            .runWith(Sink.fold(ByteString.empty)(_ ++ _))

        unzip(akkaZipped.futureValue) shouldBe inputFiles
      }
    }
  }

  private def getPathFromResources(fileName: String): Path =
    Paths.get(getClass.getClassLoader.getResource(fileName).getPath)

  private def generateInputFiles(numberOfFiles: Int, lengthOfFile: Int): Map[String, Seq[Byte]] = {
    val r = new scala.util.Random(31)
    (1 to numberOfFiles).map(number => s"file-$number" -> r.nextString(lengthOfFile).getBytes.toSeq).toMap
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

  private def unzip(bytes: ByteString): Map[String, Seq[Byte]] = {
    var result: Map[String, Seq[Byte]] = Map.empty
    val zis = new ZipInputStream(new ByteArrayInputStream(bytes.toArray))
    val buffer = new Array[Byte](1024)

    try {
      var zipEntry = zis.getNextEntry
      while (zipEntry != null) {
        val baos = new ByteArrayOutputStream()
        val name = zipEntry.getName

        var len = zis.read(buffer)

        while (len > 0) {
          baos.write(buffer, 0, len)
          len = zis.read(buffer)
        }
        result += (name -> baos.toByteArray.toSeq)
        baos.close()
        zipEntry = zis.getNextEntry
      }
    } finally {
      zis.closeEntry()
      zis.close()
    }
    result
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
