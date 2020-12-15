/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp

import java.io.IOException
import java.net.InetAddress
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.stream.IOResult
import BaseSftpSupport.{CLIENT_PRIVATE_KEY_PASSPHRASE => ClientPrivateKeyPassphrase}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Random

final class FtpStageSpec extends BaseFtpSpec with CommonFtpStageSpec
final class FtpsStageSpec extends BaseFtpsSpec with CommonFtpStageSpec
final class SftpStageSpec extends BaseSftpSpec with CommonFtpStageSpec

final class RawKeySftpSourceSpec extends BaseSftpSpec with CommonFtpStageSpec {
  override val settings = SftpSettings(
    InetAddress.getByName(HOSTNAME)
  ).withPort(PORT)
    .withCredentials(FtpCredentials.create("username", "wrong password"))
    .withStrictHostKeyChecking(false)
    .withSftpIdentity(
      SftpIdentity.createRawSftpIdentity(
        Files.readAllBytes(Paths.get(getClientPrivateKeyFile.getPath)),
        ClientPrivateKeyPassphrase
      )
    )
}

final class KeyFileSftpSourceSpec extends BaseSftpSpec with CommonFtpStageSpec {

  override val settings = SftpSettings(
    InetAddress.getByName(HOSTNAME)
  ).withPort(PORT)
    .withCredentials(FtpCredentials.create("username", "wrong password"))
    .withStrictHostKeyChecking(false)
    .withSftpIdentity(
      SftpIdentity
        .createFileSftpIdentity(getClientPrivateKeyFile.getPath, ClientPrivateKeyPassphrase)
    )
}

final class StrictHostCheckingSftpSourceSpec extends BaseSftpSpec with CommonFtpStageSpec {
  override val settings = SftpSettings(
    InetAddress.getByName(HOSTNAME)
  ).withPort(PORT)
    .withCredentials(FtpCredentials.create("username", "wrong password"))
    .withStrictHostKeyChecking(true)
    .withKnownHosts(getKnownHostsFile.getPath)
    .withSftpIdentity(
      SftpIdentity
        .createFileSftpIdentity(getClientPrivateKeyFile.getPath, ClientPrivateKeyPassphrase)
    )
}

trait CommonFtpStageSpec extends BaseSpec with Eventually {

  implicit val system = getSystem
  implicit val mat = getMaterializer
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(600, Millis))

  "FtpBrowserSource" should {
    "complete with a failed Future, when the credentials supplied were wrong" in assertAllStagesStopped {
      implicit val ec = system.getDispatcher
      listFilesWithWrongCredentials("")
        .toMat(Sink.seq)(Keep.right)
        .run()
        .failed
        .map { ex =>
          ex shouldBe a[FtpAuthenticationException]
        }
    }

    "list all files from root" in assertAllStagesStopped {
      val basePath = ""
      generateFiles(30, 10, basePath)
      val probe =
        listFiles(basePath).toMat(TestSink.probe)(Keep.right).run()
      probe.request(40).expectNextN(30)
      probe.expectComplete()
    }

    "list all files from non-root" in assertAllStagesStopped {
      val basePath = "/foo"
      generateFiles(30, 10, basePath)
      val probe =
        listFiles(basePath).toMat(TestSink.probe)(Keep.right).run()
      probe.request(40).expectNextN(30)
      probe.expectComplete()
    }

    "list only first level from base path" in assertAllStagesStopped {
      val basePath = "/foo"
      generateFiles(30, 10, basePath)
      val probe =
        listFilesWithFilter(basePath, f => false).toMat(TestSink.probe)(Keep.right).run()
      probe.request(40).expectNextN(12) // 9 files, 3 directories
      probe.expectComplete()

    }

    "list only go into second page" in assertAllStagesStopped {
      val basePath = "/foo"
      generateFiles(30, 10, basePath)
      val probe =
        listFilesWithFilter(basePath, f => f.name.contains("1"))
          .toMat(TestSink.probe)(Keep.right)
          .run()
      probe.request(40).expectNextN(21) // 9 files in root, 2 directories, 10 files in dir_1
      probe.expectComplete()

    }

    "list all files in sparse directory tree" in assertAllStagesStopped {
      putFileOnFtp("foo/bar/baz/foobar/sample")
      val probe =
        listFiles("/").toMat(TestSink.probe)(Keep.right).run()
      probe.request(2).expectNextN(1)
      probe.expectComplete()
    }

    "list all files and directories when emitTraversedDirectories is set to true" in assertAllStagesStopped {
      putFileOnFtp("foo/bar/baz/foobar/sample")
      val probe =
        listFilesWithFilter("/", _ => true, emitTraversedDirectories = true)
          .toMat(TestSink.probe)(Keep.right)
          .run()
      probe.request(10).expectNextN(5) // foo, bar, baz, foobar, and sample_1 = 5 files
      probe.expectComplete()
    }

    "retrieve relevant file attributes" in assertAllStagesStopped {
      val fileName = "sample"
      val basePath = "/"

      putFileOnFtp(fileName)

      val timestamp = System.currentTimeMillis().millis

      val files = listFiles(basePath).runWith(Sink.seq).futureValue

      files should have size 1
      inside(files.head) {
        case FtpFile(actualFileName, actualPath, isDirectory, size, lastModified, perms) =>
          actualFileName shouldBe fileName
          // actualPath shouldBe s"/$basePath$fileName"
          isDirectory shouldBe false
          size shouldBe getDefaultContent.length
          timestamp - lastModified.millis should be < 1.minute
          perms should contain allOf (PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)
      }
    }
  }

  "FtpIOSource" should {
    "retrieve a file from path as a stream of bytes" in assertAllStagesStopped {
      val fileName = "sample_io_" + Instant.now().getNano
      putFileOnFtp(fileName)
      val (result, probe) =
        retrieveFromPath(s"/$fileName").toMat(TestSink.probe)(Keep.both).run()
      probe.request(100).expectNextOrComplete()

      val expectedNumOfBytes = getDefaultContent.getBytes().length
      result.futureValue shouldBe IOResult.createSuccessful(expectedNumOfBytes)
    }

    "retrieve a file from path and offset as a stream of bytes" in assertAllStagesStopped {
      val fileName = "sample_io_" + Instant.now().getNano
      val offset = 10L
      putFileOnFtp(fileName)
      val (result, probe) =
        retrieveFromPathWithOffset(s"/$fileName", offset).toMat(TestSink.probe)(Keep.both).run()
      probe.request(100).expectNextOrComplete()

      val expectedNumOfBytes = getDefaultContent.getBytes().length - offset
      result.futureValue shouldBe IOResult.createSuccessful(expectedNumOfBytes)
    }

    "retrieve a bigger file (~2 MB) from path as a stream of bytes" in assertAllStagesStopped {
      val fileName = "sample_bigger_file_" + Instant.now().getNano
      val fileContents = new Array[Byte](2000020)
      Random.nextBytes(fileContents)
      putFileOnFtpWithContents(fileName, fileContents)
      val (result, probe) = retrieveFromPath(s"/$fileName").toMat(TestSink.probe)(Keep.both).run()
      probe.request(1000).expectNextOrComplete()

      val expectedNumOfBytes = fileContents.length
      result.futureValue shouldBe IOResult.createSuccessful(expectedNumOfBytes)
    }

    "retrieve a bigger file (~2 MB) from path and offset as a stream of bytes" in assertAllStagesStopped {
      val fileName = "sample_bigger_file_" + Instant.now().getNano
      val fileContents = new Array[Byte](2000020)
      val offset = 1000010L
      Random.nextBytes(fileContents)
      putFileOnFtpWithContents(fileName, fileContents)
      val (result, probe) =
        retrieveFromPathWithOffset(s"/$fileName", offset).toMat(TestSink.probe)(Keep.both).run()
      probe.request(1000).expectNextOrComplete()

      val expectedNumOfBytes = fileContents.length - offset
      result.futureValue shouldBe IOResult.createSuccessful(expectedNumOfBytes)
    }
  }

  "FtpBrowserSource & FtpIOSource" should {
    "work together retrieving a list of files" in assertAllStagesStopped {
      val basePath = ""
      val numOfFiles = 10
      generateFiles(numOfFiles, numOfFiles, basePath)
      val probe = listFiles(basePath)
        .mapAsyncUnordered(1)(file => retrieveFromPath(file.path, fromRoot = true).to(Sink.ignore).run())
        .toMat(TestSink.probe)(Keep.right)
        .run()
      val result = probe.request(numOfFiles + 1).expectNextN(numOfFiles)
      probe.expectComplete()

      val expectedNumOfBytes = getDefaultContent.getBytes().length * numOfFiles
      val total = result.map(_.count).sum
      total shouldBe expectedNumOfBytes
    }
  }

  "FTPIOSink" when {

    "no file is already present at the target location" should {
      "create a new file from the provided stream of bytes regardless of the append mode" in assertAllStagesStopped {
        val fileName = "sample_io_" + Instant.now().getNano
        List(true, false).foreach { mode =>
          val result =
            Source
              .single(ByteString(getDefaultContent))
              .runWith(storeToPath(s"/$fileName", mode))
              .futureValue

          val expectedNumOfBytes = getDefaultContent.getBytes().length
          result shouldBe IOResult.createSuccessful(expectedNumOfBytes)

          eventually {
            val storedContents = getFtpFileContents(fileName)
            storedContents shouldBe getDefaultContent.getBytes
          }
        }
      }
    }

    "a file is already present at the target location" should {

      val reversedLoremIpsum = getDefaultContent.reverse
      val expectedNumOfBytes = reversedLoremIpsum.length

      "overwrite it when not in append mode" in assertAllStagesStopped {
        val fileName = "sample_io_" + Instant.now().getNano
        putFileOnFtp(fileName)

        val result =
          Source
            .single(ByteString(reversedLoremIpsum))
            .runWith(storeToPath(s"/$fileName", append = false))
            .futureValue

        result shouldBe IOResult.createSuccessful(expectedNumOfBytes)

        eventually {
          val storedContents = getFtpFileContents(fileName)
          storedContents shouldBe reversedLoremIpsum.getBytes
        }

      }

      "append to its contents when in append mode" in assertAllStagesStopped {
        val fileName = "sample_io_" + Instant.now().getNano
        putFileOnFtp(fileName)

        val result =
          Source
            .single(ByteString(reversedLoremIpsum))
            .runWith(storeToPath(s"/$fileName", append = true))
            .futureValue

        result shouldBe IOResult.createSuccessful(expectedNumOfBytes)

        eventually {
          val storedContents = getFtpFileContents(fileName)
          storedContents shouldBe getDefaultContent.getBytes ++ reversedLoremIpsum.getBytes
        }
      }
    }

  }

  it should {

    "write a bigger file (~2 MB) to a path from a stream of bytes" in assertAllStagesStopped {
      val fileName = "sample_bigger_file_" + Instant.now().getNano
      val fileContents = new Array[Byte](2000020)
      Random.nextBytes(fileContents)

      val result = Source[Byte](fileContents.toList)
        .grouped(8192)
        .map(s => ByteString.apply(s.toArray))
        .runWith(storeToPath(s"/$fileName", append = false))
        .futureValue

      val expectedNumOfBytes = fileContents.length
      result shouldBe IOResult.createSuccessful(expectedNumOfBytes)

      eventually {
        val storedContents = getFtpFileContents(fileName)
        storedContents.length shouldBe fileContents.length
        storedContents shouldBe fileContents
      }
    }

    "fail and report the exception in the result status if upstream fails" in assertAllStagesStopped {
      val fileName = "sample_io_upstream_" + Instant.now().getNano
      val brokenSource = Source(10.to(0, -1)).map(x => ByteString(10 / x))

      val result = brokenSource.runWith(storeToPath(s"/$fileName", append = false)).futureValue

      result.status.failed.get shouldBe a[ArithmeticException]
    }

    "fail and report the exception in the result status if connection fails" ignore { // TODO Fails too often on Travis: assertAllStagesStopped {
      def waitForUploadToStart(fileName: String) =
        eventually {
          noException should be thrownBy getFtpFileContents(fileName)
          getFtpFileContents(fileName).length shouldBe >(0)
        }

      val fileName = "sample_io_connection"
      val infiniteSource = Source.repeat(ByteString(0x00))

      val future = infiniteSource.runWith(storeToPath(s"/$fileName", append = false))
      waitForUploadToStart(fileName)
      // stopServer()
      val result = future.futureValue
      // startServer()

      result.status.failed.get shouldBe a[Exception]
    }
  }

  "FtpRemoveSink" should {
    "remove a file" in { // TODO Fails too often on Travis: assertAllStagesStopped {
      val fileName = "sample_io_" + Instant.now().getNano
      putFileOnFtp(fileName)

      val source = listFiles("/")

      eventually {
        source.map(_.name).runWith(Sink.head).futureValue shouldBe fileName
      }

      val result = source.runWith(remove()).futureValue

      result shouldBe IOResult.createSuccessful(1)

      eventually {
        fileExists(fileName) shouldBe false
      }
    }

    "fail when the file does not exist" in {
      val fileName = "sample_io_" + Instant.now().getNano
      val file = FtpFile(
        name = fileName,
        path = s"/$fileName",
        isDirectory = false,
        size = 999,
        lastModified = 0,
        permissions = Set.empty
      )

      val result = Source
        .single(file)
        .runWith(remove())
        .futureValue

      val ex = result.status.failed.get
      ex shouldBe an[IOException]
      ex should (have message s"Could not delete /$fileName" or have message "No such file")
    }
  }

  "FtpMoveSink" should {
    "move a file" in { // TODO Fails too often on Travis: assertAllStagesStopped {
      val fileName = "sample_io_" + Instant.now().getNano
      val fileName2 = "sample_io2_" + Instant.now().getNano
      putFileOnFtp(fileName)

      val source = listFiles("/")

      eventually {
        source.map(_.name).runWith(Sink.head).futureValue shouldBe fileName
      }

      val result = source.runWith(move(_ => fileName2)).futureValue

      result shouldBe IOResult.createSuccessful(1)

      eventually {
        fileExists(fileName) shouldBe false
        fileExists(fileName2) shouldBe true
      }
    }

    "fail when the source file does not exist" in {
      val fileName = "sample_io_" + Instant.now().getNano
      val fileName2 = "sample_io2_" + Instant.now().getNano

      val file = FtpFile(
        name = fileName,
        path = s"/$fileName",
        isDirectory = false,
        size = 999,
        lastModified = 0,
        permissions = Set.empty
      )

      val result = Source
        .single(file)
        .runWith(move(_ => fileName2))
        .futureValue

      val ex = result.status.failed.get
      ex shouldBe an[IOException]
      ex should (have message s"Could not move /$fileName" or have message "No such file")
    }
  }

  "FtpDirectoryOperationsSource" should {
    "make a directory in current path" in {
      val basePath = "/"
      val name = "sample_dir_" + Instant.now().getNano

      val source = mkdir(basePath, name)

      val res = source.runWith(Sink.head)

      Await.result(res, Duration(1, TimeUnit.MINUTES))

      val listing = listFilesWithFilter(
        basePath = "/",
        branchSelector = _ => true,
        emitTraversedDirectories = true
      ).runWith(Sink.seq)

      val listingRes: immutable.Seq[FtpFile] = Await.result(listing, Duration(1, TimeUnit.MINUTES))

      listingRes.map(_.name) should contain allElementsOf Seq(name)
    }

    "make a directory in given path" in {
      val basePath = "/"
      val name = "sample_dir_" + Instant.now().getNano
      val innerDirPath = s"$basePath/$name"
      val innerDirName = "sample_dir_" + Instant.now().getNano

      val source = mkdir(basePath, name)
      val innerSource = mkdir(innerDirPath, innerDirName)

      implicit val ec: ExecutionContextExecutor = mat.executionContext

      val res = for {
        x <- source.runWith(Sink.head)
        y <- innerSource.runWith(Sink.head)
      } yield (x, y)

      Await.result(res, Duration(1, TimeUnit.MINUTES))

      val listing = listFilesWithFilter(
        basePath = "/",
        branchSelector = _ => true,
        emitTraversedDirectories = true
      ).runWith(Sink.seq)

      val listingRes: immutable.Seq[FtpFile] = Await.result(listing, Duration(1, TimeUnit.MINUTES))

      listingRes.map(_.name) should contain allElementsOf Seq(name)

      val listingOnlyInnerDir = listFilesWithFilter(
        basePath = innerDirPath,
        branchSelector = _ => true,
        emitTraversedDirectories = true
      ).runWith(Sink.seq)

      val listingInnerDirRes: immutable.Seq[FtpFile] =
        Await.result(listingOnlyInnerDir, Duration(1, TimeUnit.MINUTES))

      listingInnerDirRes.map(_.name) should contain allElementsOf Seq(innerDirName)

    }
  }
}
