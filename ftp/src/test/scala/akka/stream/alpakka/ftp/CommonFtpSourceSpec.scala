/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp

import akka.stream.IOResult
import akka.stream.scaladsl.Keep
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

  "FtpBrowserSource" when {
    "initialized from root directory" should {
      "list all files" in {
        checkFiles(
          numFiles = 30,
          pageSize = 10,
          demand = 40,
          basePath = ""
        )
      }
    }
    "initialized from non-root directory" should {
      "list all files" in {
        checkFiles(
          numFiles = 30,
          pageSize = 10,
          demand = 40,
          basePath = "/foo"
        )
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
  }

  private[this] def checkFiles(numFiles: Int, pageSize: Int, demand: Int, basePath: String) = {
    generateFiles(numFiles, pageSize, basePath)
    val (_, probe) =
      listFiles(basePath).toMat(TestSink.probe)(Keep.both).run()
    probe.request(demand).expectNextN(numFiles.toLong)
    ()
  }

}
