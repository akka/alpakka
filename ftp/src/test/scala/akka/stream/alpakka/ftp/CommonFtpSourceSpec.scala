/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp

import akka.stream.IOResult
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSink

final class FtpSourceSpec extends BaseFtpSpec with CommonFtpSourceSpec
final class SftpSourceSpec extends BaseSftpSpec with CommonFtpSourceSpec
final class FtpsSourceSpec extends BaseFtpsSpec with CommonFtpSourceSpec {
  setAuthValue("TLS")
  setUseImplicit(false)
}

trait CommonFtpSourceSpec extends BaseSpec {

  implicit val system = getSystem
  implicit val mat = getMaterializer

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
  }

  "FtpBrowserSource & FtpIOSource" should {
    "work together retrieving a list of files" in {
      val basePath = ""
      val numOfFiles = 30
      generateFiles(numOfFiles, 10, basePath)
      val probe = listFiles(basePath)
        .mapAsyncUnordered(4)(file => retrieveFromPath(file.path).to(Sink.ignore).run())
        .toMat(TestSink.probe)(Keep.right)
        .run()
      val result = probe.request(100).expectNextN(30)
      probe.expectComplete()

      val expectedNumOfBytes = getLoremIpsum.getBytes().length * numOfFiles
      val total = result.foldLeft(0L) {
        case (acc, next) => acc + next.count
      }
      total shouldBe expectedNumOfBytes
    }
  }

}
