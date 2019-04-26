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

trait BaseSftpSpec extends BaseSftpSupport with BaseSpec {

  val settings = SftpSettings(
    InetAddress.getByName(HOSTNAME)
  ).withPort(PORT)
    .withCredentials(FtpCredentials.create("username", "userpass"))
    .withStrictHostKeyChecking(false)

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Sftp.ls(ROOT_PATH + basePath, settings)

  protected def listFilesWithFilter(basePath: String,
                                    branchSelector: FtpFile => Boolean,
                                    emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Sftp.ls(ROOT_PATH + basePath, settings, branchSelector, emitTraversedDirectories)

  protected def retrieveFromPath(path: String, fromRoot: Boolean = false): Source[ByteString, Future[IOResult]] = {
    val finalPath = if (fromRoot) path else ROOT_PATH + path
    Sftp.fromPath(finalPath, settings)
  }

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]] =
    Sftp.toPath(ROOT_PATH + path, settings, append)

  protected def remove(): Sink[FtpFile, Future[IOResult]] =
    Sftp.remove(settings)

  protected def move(destinationPath: FtpFile => String): Sink[FtpFile, Future[IOResult]] =
    Sftp.move(file => ROOT_PATH + destinationPath(file), settings)
}
