/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp
import java.net.InetAddress

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.alpakka.ftp.scaladsl.Sftp
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

trait BaseSftpSpec extends SftpSupportImpl with BaseSpec {

  val settings = SftpSettings(
    InetAddress.getByName("localhost")
  ).withPort(getPort)
    .withStrictHostKeyChecking(false)

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Sftp.ls(basePath, settings)

  protected def listFilesWithFilter(basePath: String,
                                    branchSelector: FtpFile => Boolean,
                                    emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Sftp.ls(basePath, settings, branchSelector, emitTraversedDirectories)

  protected def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Sftp.fromPath(path, settings)

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]] =
    Sftp.toPath(path, settings, append)

  protected def remove(): Sink[FtpFile, Future[IOResult]] =
    Sftp.remove(settings)

  protected def move(destinationPath: FtpFile => String): Sink[FtpFile, Future[IOResult]] =
    Sftp.move(destinationPath, settings)
}
