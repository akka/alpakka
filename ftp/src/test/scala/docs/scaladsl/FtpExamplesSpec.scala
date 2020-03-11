/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl
import java.io.PrintWriter
import java.net.InetAddress

import akka.stream.Materializer
import akka.stream.alpakka.ftp.{BaseFtpSupport, FtpSettings}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestKit
import org.apache.commons.net.PrintCommandListener
import org.apache.commons.net.ftp.FTPClient
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class FtpExamplesSpec
    extends BaseFtpSupport
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with LogCapturing {

  implicit val materializer: Materializer = getMaterializer

  override protected def afterAll(): Unit = {
    getRootDir.resolve("file.txt").toFile.delete()
    getRootDir.resolve("file.txt.gz").toFile.delete()
    TestKit.shutdownActorSystem(getSystem)
    super.afterAll()
  }

  def ftpSettings = {
    //#create-settings
    val ftpSettings = FtpSettings
      .create(InetAddress.getByName(HOSTNAME))
      .withPort(PORT)
      .withCredentials(CREDENTIALS)
      .withBinary(true)
      .withPassiveMode(true)
      // only useful for debugging
      .withConfigureConnection((ftpClient: FTPClient) => {
        ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true))
      })
    //#create-settings
    ftpSettings
  }

  "a file" should {
    "be stored" in assertAllStagesStopped {
      //#storing
      import akka.stream.IOResult
      import akka.stream.alpakka.ftp.scaladsl.Ftp
      import akka.util.ByteString
      import scala.concurrent.Future

      val result: Future[IOResult] = Source
        .single(ByteString("this is the file contents"))
        .runWith(Ftp.toPath("file.txt", ftpSettings))
      //#storing

      val ioResult = result.futureValue(timeout(Span(1, Seconds)))
      ioResult should be(IOResult.createSuccessful(25))

      val p = fileExists("file.txt")
      p should be(true)

    }

    "be gzipped" in assertAllStagesStopped {
      import akka.stream.IOResult
      import akka.stream.alpakka.ftp.scaladsl.Ftp
      import akka.util.ByteString
      import scala.concurrent.Future

      //#storing

      // Create a gzipped target file
      import akka.stream.scaladsl.Compression
      val result: Future[IOResult] = Source
        .single(ByteString("this is the file contents" * 50))
        .via(Compression.gzip)
        .runWith(Ftp.toPath("file.txt.gz", ftpSettings))
      //#storing

      val ioResult = result.futureValue(timeout(Span(1, Seconds)))
      ioResult should be(IOResult.createSuccessful(61))

      val p = fileExists("file.txt.gz")
      p should be(true)

    }
  }

}
