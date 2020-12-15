/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp
import java.net.InetAddress

import akka.{Done, NotUsed}
import akka.stream.IOResult
import akka.stream.alpakka.ftp.scaladsl.Sftp
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

trait BaseSftpSpec extends BaseSftpSupport with BaseSpec {

  private def createSettings(credentials: FtpCredentials): SftpSettings =
    SftpSettings(
      InetAddress.getByName(HOSTNAME)
    ).withPort(PORT)
      .withCredentials(credentials)
      .withStrictHostKeyChecking(false)

  val settings = createSettings(CREDENTIALS)
  val wrongSettings = createSettings(WRONG_CREDENTIALS)

  protected def listFilesWithWrongCredentials(basePath: String): Source[FtpFile, NotUsed] =
    Sftp.ls(ROOT_PATH + basePath, wrongSettings)

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Sftp.ls(ROOT_PATH + basePath, settings)

  protected def listFilesWithFilter(
      basePath: String,
      branchSelector: FtpFile => Boolean,
      emitTraversedDirectories: Boolean
  ): Source[FtpFile, NotUsed] =
    Sftp.ls(ROOT_PATH + basePath, settings, branchSelector, emitTraversedDirectories)

  protected def retrieveFromPath(
      path: String,
      fromRoot: Boolean = false
  ): Source[ByteString, Future[IOResult]] = {
    val finalPath = if (fromRoot) path else ROOT_PATH + path
    Sftp.fromPath(finalPath, settings)
  }

  protected def retrieveFromPathWithOffset(
      path: String,
      offset: Long
  ): Source[ByteString, Future[IOResult]] =
    Sftp.fromPath(ROOT_PATH + path, settings, 8192, offset)

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]] =
    Sftp.toPath(ROOT_PATH + path, settings, append)

  protected def remove(): Sink[FtpFile, Future[IOResult]] =
    Sftp.remove(settings)

  protected def move(destinationPath: FtpFile => String): Sink[FtpFile, Future[IOResult]] =
    Sftp.move(file => ROOT_PATH + destinationPath(file), settings)

  protected def mkdir(basePath: String, name: String): Source[Done, NotUsed] =
    Sftp.mkdir(ROOT_PATH + basePath, name, settings)
}
