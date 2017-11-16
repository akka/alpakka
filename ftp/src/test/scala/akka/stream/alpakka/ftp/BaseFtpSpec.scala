/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.util.ByteString
import scala.concurrent.Future
import java.net.InetAddress

trait BaseFtpSpec extends PlainFtpSupportImpl with BaseSpec {

  val settings = FtpSettings(
    InetAddress.getByName("localhost"),
    getPort,
    AnonFtpCredentials,
    binary = true,
    passiveMode = true
  )

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Ftp.ls(basePath, settings)

  protected def listFilesWithFilter(basePath: String, branchSelector: FtpFile => Boolean): Source[FtpFile, NotUsed] =
    Ftp.ls(basePath, settings, branchSelector)

  protected def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Ftp.fromPath(path, settings)

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]] =
    Ftp.toPath(path, settings, append)
}
