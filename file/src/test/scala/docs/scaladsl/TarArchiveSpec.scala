/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.io._
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.Comparator

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.{Archive, Directory}
import akka.stream.alpakka.file.{TarArchiveMetadata, TarReaderException}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class TarArchiveSpec
    extends TestKit(ActorSystem("TarArchiveSpec"))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with LogCapturing
    with Eventually
    with IntegrationPatience {

  implicit val ec: ExecutionContext = system.dispatcher

  private val collectByteString: Sink[ByteString, Future[ByteString]] = Sink.fold(ByteString.empty)(_ ++ _)

  "tar writer" should {
    "pass empty stream" in {
      val sources = Source.empty
      val tarFlow = Archive.tar()

      val akkaTarred: Future[ByteString] =
        sources
          .via(tarFlow)
          .runWith(collectByteString)

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

      val lastModification = Instant.now
      // #sample-tar
      val filesStream = Source(
        List(
          (TarArchiveMetadata.directory("subdir", lastModification), Source.empty),
          (TarArchiveMetadata("subdir", "akka_full_color.svg", fileSize1, lastModification), fileStream1),
          (TarArchiveMetadata("akka_icon_reverse.svg", fileSize2, lastModification), fileStream2)
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

      result.futureValue.count shouldBe 4096
      resultGz.futureValue.count should not be 4096

      untar(Paths.get("result.tar").toRealPath(), "xf").foreach(
        _ shouldBe Map(
          "subdir/akka_full_color.svg" -> fileContent1,
          "akka_icon_reverse.svg" -> fileContent2
        )
      )
      untar(Paths.get("result.tar.gz").toRealPath(), "xfz").foreach(
        _ shouldBe Map(
          "subdir/akka_full_color.svg" -> fileContent1,
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

      val file = (TarArchiveMetadata(fileName, fileBytes.size), Source.single(fileBytes))
      val filesStream = Source.single(file)
      val result = filesStream
        .via(Archive.tar())
        .runWith(FileIO.toPath(Paths.get("result.tar")))
      result.futureValue.count shouldBe 1024

      untar(Paths.get("result.tar").toRealPath(), "xf").foreach(_ shouldBe Map(fileName -> fileBytes))

      //cleanup
      new File("result.tar").delete()
    }

    "fail if provided size and actual size do not match" in {
      val file = (TarArchiveMetadata("file.txt", 10L), Source.single(ByteString("too long")))
      val filesStream = Source.single(file)
      val result = filesStream.via(Archive.tar()).runWith(Sink.ignore)
      an[IllegalStateException] should be thrownBy (throw result.failed.futureValue)
    }
  }

  "tar reader" should {
    val tenDigits = ByteString("1234567890")
    val metadata1 = TarArchiveMetadata("dir/file1.txt", tenDigits.length)

    val oneFileArchive = {
      Source
        .single(metadata1 -> Source.single(tenDigits))
        .via(Archive.tar())
        .runWith(collectByteString)
    }

    "emit one file" in {
      val tar =
        Source
          .future(oneFileArchive)
          .via(Archive.tarReader())
          .mapAsync(1) {
            case in @ (metadata, source) =>
              source.runWith(collectByteString).map { bs =>
                metadata -> bs
              }
          }
          .runWith(Sink.head)
      val result = tar.futureValue
      result shouldBe metadata1 -> tenDigits
    }

    "document its use" in {
      // #tar-reader
      val bytesSource: Source[ByteString, NotUsed] = // ???
        // #tar-reader
        Source.future(oneFileArchive)
      val target = Files.createTempDirectory("alpakka-tar-")

      // #tar-reader
      val tar =
        bytesSource
          .via(Archive.tarReader())
          .mapAsync(1) {
            case (metadata, source) =>
              val targetFile = target.resolve(metadata.filePath)
              if (metadata.isDirectory) {
                Source
                  .single(targetFile)
                  .via(Directory.mkdirs())
                  .runWith(Sink.ignore)
              } else {
                // create the target directory
                Source
                  .single(targetFile.getParent)
                  .via(Directory.mkdirs())
                  .runWith(Sink.ignore)
                  .map { _ =>
                    // stream the file contents to a local file
                    source.runWith(FileIO.toPath(targetFile))
                  }
              }
          }
          .runWith(Sink.ignore)
      // #tar-reader
      tar.futureValue shouldBe Done
      val file: File = target.resolve("dir/file1.txt").toFile
      eventually {
        file.exists() shouldBe true
      }
    }

    "emit empty file" in {
      val metadataEmpty = TarArchiveMetadata("dir/empty.txt", 0L)
      val tar =
        Source
          .single(metadataEmpty -> Source.single(ByteString.empty))
          .via(Archive.tar())
          .via(Archive.tarReader())
          .mapAsync(1) {
            case (metadata, source) =>
              source.runWith(collectByteString).map { bs =>
                metadata -> bs
              }
          }
          .runWith(Sink.head)
      val result = tar.futureValue
      result shouldBe metadataEmpty -> ByteString.empty
    }

    "emit two files" in {
      val Seq((name1, file1), (name2, file2)) = generateInputFiles(2, 400)

      val metadata1 = TarArchiveMetadata(name1, file1.length)
      val metadata2 = TarArchiveMetadata(name2, file2.length)
      val tar = Source(
        immutable.Seq(
          metadata1 -> Source.single(file1),
          metadata2 -> Source.single(file2)
        )
      ).via(Archive.tar())
        // emit in short byte strings
        .mapConcat(_.sliding(2, 2).toList)
        .via(Archive.tarReader())
        .mapAsync(1) {
          case (metadata, source) =>
            source.runWith(collectByteString).map { bs =>
              (metadata -> bs)
            }
        }
        .runWith(Sink.seq)
      val result = tar.futureValue
      result(0) shouldBe metadata1 -> file1
      result(1) shouldBe metadata2 -> file2
    }

    "emit empty and another file" in {
      val metadata1 = TarArchiveMetadata("empty.txt", 0)
      val metadata2 = TarArchiveMetadata("file2.txt", tenDigits.length)
      val tarFile = Source(
        immutable.Seq(
          metadata1 -> Source.single(ByteString.empty),
          metadata2 -> Source.single(tenDigits)
        )
      ).via(Archive.tar())
        // swallow the whole input as one ByteString
        .runWith(collectByteString)

      val tar = Source
        .future(tarFile)
        .via(Archive.tarReader())
        .mapAsync(1) {
          case (metadata, source) =>
            source.runWith(collectByteString).map { bs =>
              metadata -> bs
            }
        }
        .runWith(Sink.seq)
      val result = tar.futureValue
      result(0) shouldBe metadata1 -> ByteString.empty
      result(1) shouldBe metadata2 -> tenDigits
    }

    "fail for incomplete header" in {
      val input = oneFileArchive.map(bs => bs.take(500))
      val tar = Source
        .future(input)
        .via(Archive.tarReader())
        .runWith(Sink.ignore)
      val error = tar.failed.futureValue
      error shouldBe a[TarReaderException]
      error.getMessage shouldBe "incomplete tar header: received 500 bytes, expected 512 bytes"
    }

    "fail for incomplete file" in {
      val input = oneFileArchive.map(bs => bs.take(518))
      val tar = Source
        .future(input)
        .via(Archive.tarReader())
        .mapAsync(1) {
          case (metadata, source) =>
            source.runWith(Sink.ignore)
        }
        .runWith(Sink.ignore)
      val error = tar.failed.futureValue
      error shouldBe a[TarReaderException]
      error.getMessage shouldBe "incomplete tar file contents for [dir/file1.txt] expected 10 bytes, received 6 bytes"
    }

    "fail for incomplete trailer" in {
      val input = oneFileArchive.map(bs => bs.take(535))
      val tar = Source
        .future(input)
        .via(Archive.tarReader())
        .mapAsync(1) {
          case (metadata, source) =>
            source.runWith(Sink.ignore)
        }
        .runWith(Sink.ignore)
      val error = tar.failed.futureValue
      error shouldBe a[TarReaderException]
      error.getMessage shouldBe "incomplete tar file trailer for [dir/file1.txt] expected 502 bytes, received 13 bytes"
    }

    "accept empty input" in {
      val tar = Source
        .single(ByteString.empty)
        .via(Archive.tarReader())
        .runWith(Sink.seq)
      tar.futureValue shouldBe Symbol("empty")
    }

    "fail on missing sub source subscription" in {
      val tar =
        Source
          .future(oneFileArchive)
          .mapConcat(_.sliding(2, 2).toList)
          .via(Archive.tarReader())
          .runWith(Sink.ignore)
      val error = tar.failed.futureValue
      error shouldBe a[TarReaderException]
      error.getMessage shouldBe "The tar content source was not subscribed to within 5000 milliseconds, it must be subscribed to to progress tar file reading."
    }
  }

  "advanced tar reading" should {
    "allow tar files in tar files to be extracted in a single flow" in {
      val tenDigits = ByteString("1234567890")
      val metadata1 = TarArchiveMetadata("dir/file1.txt", tenDigits.length)

      val nestedArchive = {
        Source
          .single(metadata1 -> Source.single(tenDigits))
          .via(Archive.tar())
          .runWith(collectByteString)
      }
      val outerArchive: Future[ByteString] =
        Source
          .future(nestedArchive)
          .map(bs => TarArchiveMetadata("nested.tar", bs.size) -> Source.single(bs))
          .via(Archive.tar())
          .runWith(collectByteString)

      val res = Source
        .future(outerArchive)
        .mapConcat(_.sliding(100, 100).toList)
        .via(untar())
        .map(_.filePathName)
        .runWith(Sink.seq)

      res.futureValue shouldBe Seq("nested.tar", "file1.txt")
    }

    def untar(): Flow[ByteString, TarArchiveMetadata, NotUsed] = {
      Archive
        .tarReader()
        .log("untar")
        .mapAsync(1) {
          case (metadata, source) if metadata.filePath.endsWith(".tar") =>
            val contents: Source[TarArchiveMetadata, NotUsed] = Source.single(metadata).concat(source.via(untar()))
            Future.successful(contents)
          case (metadata, source) =>
            source
              .runWith(Sink.ignore)
              .map { _ =>
                Source.single(metadata)
              }
        }
        .flatMapConcat(identity)
        .log("untarred")
    }
  }

  private def getPathFromResources(fileName: String): Path =
    Paths.get(getClass.getClassLoader.getResource(fileName).toURI)

  private def generateInputFiles(numberOfFiles: Int, lengthOfFile: Int): immutable.Seq[(String, ByteString)] = {
    val r = new scala.util.Random(31)
    (1 to numberOfFiles)
      .map(number => s"file-$number" -> ByteString.fromArray(r.nextString(lengthOfFile).getBytes))
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
