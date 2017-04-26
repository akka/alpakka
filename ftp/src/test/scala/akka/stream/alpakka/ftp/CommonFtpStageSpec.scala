/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp

import java.io.File
import java.net.InetAddress
import java.nio.file.{Files, Paths}

import akka.stream.IOResult
import akka.stream.alpakka.ftp.FtpCredentials.NonAnonFtpCredentials
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import org.scalatest.time.{Millis, Seconds, Span}
import scala.concurrent.duration._
import scala.util.Random
import java.nio.file.attribute.PosixFilePermission

final class FtpStageSpec extends BaseFtpSpec with CommonFtpStageSpec
final class SftpStageSpec extends BaseSftpSpec with CommonFtpStageSpec
final class FtpsStageSpec extends BaseFtpsSpec with CommonFtpStageSpec {
  setAuthValue("TLS")
  setUseImplicit(false)
}

final class RawKeySftpSourceSpec extends BaseSftpSpec with CommonFtpStageSpec {
  override val settings = SftpSettings(
    InetAddress.getByName("localhost"),
    getPort,
    NonAnonFtpCredentials("different user and password", "will fail password auth"),
    strictHostKeyChecking = false,
    knownHosts = None,
    Some(RawKeySftpIdentity("id", Files.readAllBytes(Paths.get("ftp/src/test/resources/client.pem"))))
  )
}

final class KeyFileSftpSourceSpec extends BaseSftpSpec with CommonFtpStageSpec {
  override val settings = SftpSettings(
    InetAddress.getByName("localhost"),
    getPort,
    NonAnonFtpCredentials("different user and password", "will fail password auth"),
    strictHostKeyChecking = false,
    knownHosts = None,
    Some(KeyFileSftpIdentity("ftp/src/test/resources/client.pem"))
  )
}

final class StrictHostCheckingSftpSourceSpec extends BaseSftpSpec with CommonFtpStageSpec {
  override val settings = SftpSettings(
    InetAddress.getByName("localhost"),
    getPort,
    NonAnonFtpCredentials("different user and password", "will fail password auth"),
    strictHostKeyChecking = true,
    knownHosts = Some(new File("ftp/src/test/resources/known_hosts").getAbsolutePath),
    Some(KeyFileSftpIdentity("ftp/src/test/resources/client.pem", None, None)),
    options = Map("HostKeyAlgorithms" -> "+ssh-dss")
  )
}

trait CommonFtpStageSpec extends BaseSpec {

  implicit val system = getSystem
  implicit val mat = getMaterializer
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(300, Millis))

  "FtpBrowserSource" should {
    "list all files from root" in {
      val basePath = ""
      generateFiles(30, 10, basePath)
      val probe =
        listFiles(basePath).toMat(TestSink.probe)(Keep.right).run()
      probe.request(40).expectNextN(30)
      probe.expectComplete()
    }

    "list all files from non-root" in {
      val basePath = "/foo"
      generateFiles(30, 10, basePath)
      val probe =
        listFiles(basePath).toMat(TestSink.probe)(Keep.right).run()
      probe.request(40).expectNextN(30)
      probe.expectComplete()
    }

    "list all files in sparse directory tree" in {
      val deepDir = "/foo/bar/baz/foobar"
      val basePath = "/"
      generateFiles(1, -1, deepDir)
      val probe =
        listFiles(basePath).toMat(TestSink.probe)(Keep.right).run()
      probe.request(2).expectNextN(1)
      probe.expectComplete()
    }

    "retrieve relevant file attributes" in {
      val fileName = "sample"
      val basePath = "/"

      putFileOnFtp(FtpBaseSupport.FTP_ROOT_DIR, fileName)

      val timestamp = System.currentTimeMillis().millis

      val files = listFiles(basePath).runWith(Sink.seq).futureValue

      files should have size 1
      inside(files.head) {
        case FtpFile(actualFileName, actualPath, isDirectory, size, lastModified, perms) ⇒
          actualFileName shouldBe fileName
          actualPath shouldBe s"$basePath$fileName"
          isDirectory shouldBe false
          size shouldBe getLoremIpsum.length
          timestamp - lastModified.millis should be < 1.minute
          perms should contain allOf (PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)
      }
    }
  }

  "FtpIOSource" should {
    "retrieve a file from path as a stream of bytes" in {
      val fileName = "sample_io"
      putFileOnFtp(FtpBaseSupport.FTP_ROOT_DIR, fileName)
      val (result, probe) =
        retrieveFromPath(s"/$fileName").toMat(TestSink.probe)(Keep.both).run()
      probe.request(100).expectNextOrComplete()

      val expectedNumOfBytes = getLoremIpsum.getBytes().length
      result.futureValue shouldBe IOResult.createSuccessful(expectedNumOfBytes)
    }

    "retrieve a bigger file (~2 MB) from path as a stream of bytes" in {
      val fileName = "sample_bigger_file"
      val fileContents = new Array[Byte](2000020)
      Random.nextBytes(fileContents)
      putFileOnFtpWithContents(FtpBaseSupport.FTP_ROOT_DIR, fileName, fileContents)
      val (result, probe) = retrieveFromPath(s"/$fileName").toMat(TestSink.probe)(Keep.both).run()
      probe.request(1000).expectNextOrComplete()

      val expectedNumOfBytes = fileContents.length
      result.futureValue shouldBe IOResult.createSuccessful(expectedNumOfBytes)
    }
  }

  "FtpBrowserSource & FtpIOSource" should {
    "work together retrieving a list of files" in {
      val basePath = ""
      val numOfFiles = 20
      generateFiles(numOfFiles, 10, basePath)
      val probe = listFiles(basePath)
        .mapAsyncUnordered(1)(file => retrieveFromPath(file.path).to(Sink.ignore).run())
        .toMat(TestSink.probe)(Keep.right)
        .run()
      val result = probe.request(21).expectNextN(20)
      probe.expectComplete()

      val expectedNumOfBytes = getLoremIpsum.getBytes().length * numOfFiles
      val total = result.map(_.count).sum
      total shouldBe expectedNumOfBytes
    }
  }

  "FTPIOSink" when {
    val fileName = "sample_io"

    "no file is already present at the target location" should {
      "create a new file from the provided stream of bytes regardless of the append mode" in {
        List(true, false).foreach { mode ⇒
          val result = Source.single(ByteString(getLoremIpsum)).runWith(storeToPath(s"/$fileName", mode)).futureValue

          val expectedNumOfBytes = getLoremIpsum.getBytes().length
          result shouldBe IOResult.createSuccessful(expectedNumOfBytes)

          val storedContents = getFtpFileContents(FtpBaseSupport.FTP_ROOT_DIR, fileName)
          storedContents shouldBe getLoremIpsum.getBytes

          cleanFiles()
        }
      }
    }

    "a file is already present at the target location" should {

      val reversedLoremIpsum = getLoremIpsum.reverse
      val expectedNumOfBytes = reversedLoremIpsum.length

      "overwrite it when not in append mode" in {
        putFileOnFtp(FtpBaseSupport.FTP_ROOT_DIR, fileName)

        val result =
          Source.single(ByteString(reversedLoremIpsum)).runWith(storeToPath(s"/$fileName", append = false)).futureValue

        result shouldBe IOResult.createSuccessful(expectedNumOfBytes)

        val storedContents = getFtpFileContents(FtpBaseSupport.FTP_ROOT_DIR, fileName)
        storedContents shouldBe reversedLoremIpsum.getBytes
      }

      "append to its contents when in append mode" in {
        putFileOnFtp(FtpBaseSupport.FTP_ROOT_DIR, fileName)

        val result =
          Source.single(ByteString(reversedLoremIpsum)).runWith(storeToPath(s"/$fileName", append = true)).futureValue

        result shouldBe IOResult.createSuccessful(expectedNumOfBytes)

        val storedContents = getFtpFileContents(FtpBaseSupport.FTP_ROOT_DIR, fileName)

        storedContents shouldBe getLoremIpsum.getBytes ++ reversedLoremIpsum.getBytes
      }
    }
  }

  it should {
    "write a bigger file (~2 MB) to a path from a stream of bytes" in {
      val fileName = "sample_bigger_file"
      val fileContents = new Array[Byte](2000020)
      Random.nextBytes(fileContents)

      val result = Source[Byte](fileContents.toList)
        .grouped(8192)
        .map(s ⇒ ByteString.apply(s.toArray))
        .runWith(storeToPath(s"/$fileName", append = false))
        .futureValue

      val expectedNumOfBytes = fileContents.length
      result shouldBe IOResult.createSuccessful(expectedNumOfBytes)

      val storedContents = getFtpFileContents(FtpBaseSupport.FTP_ROOT_DIR, fileName)
      storedContents shouldBe fileContents
    }

    "fail and report the exception in the result status if upstream fails" in {
      val fileName = "sample_io"
      val brokenSource = Source(10.to(0, -1)).map(x ⇒ ByteString(10 / x))

      val result = brokenSource.runWith(storeToPath(s"/$fileName", append = false)).futureValue

      result.status.failed.get shouldBe a[ArithmeticException]
    }
  }

}
