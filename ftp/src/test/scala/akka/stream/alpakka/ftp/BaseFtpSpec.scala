/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.util.ByteString
import scala.concurrent.Future
import java.net.InetAddress

trait BaseFtpSpec extends PlainFtpSupportImpl with BaseSpec {

  val settings = FtpSettings(
    InetAddress.getByName("localhost")
  ).withPort(getPort)
    .withBinary(true)
    .withPassiveMode(true)

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Ftp.ls(basePath, settings)

  protected def listFilesWithFilter(basePath: String,
                                    branchSelector: FtpFile => Boolean,
                                    emitTraversedDirectories: Boolean = false): Source[FtpFile, NotUsed] =
    Ftp.ls(basePath, settings, branchSelector, emitTraversedDirectories)

  protected def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Ftp.fromPath(path, settings)

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]] =
    Ftp.toPath(path, settings, append)

  protected def remove(): Sink[FtpFile, Future[IOResult]] =
    Ftp.remove(settings)

  protected def move(destinationPath: FtpFile => String): Sink[FtpFile, Future[IOResult]] =
    Ftp.move(destinationPath, settings)
}
