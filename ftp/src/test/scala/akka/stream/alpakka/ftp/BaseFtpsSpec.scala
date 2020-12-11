/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp
import java.net.InetAddress

import akka.{Done, NotUsed}
import akka.stream.IOResult
import akka.stream.alpakka.ftp.scaladsl.Ftps
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

trait BaseFtpsSpec extends BaseFtpSupport with BaseSpec {

  private def createSettings(credentials: FtpCredentials): FtpsSettings =
    FtpsSettings(
      InetAddress.getByName(HOSTNAME)
    ).withPort(PORT)
      .withCredentials(credentials)
      .withBinary(true)
      .withPassiveMode(true)

  val settings = createSettings(CREDENTIALS)
  val wrongSettings = createSettings(WRONG_CREDENTIALS)

  protected def listFilesWithWrongCredentials(basePath: String): Source[FtpFile, NotUsed] =
    Ftps.ls(basePath, wrongSettings)

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Ftps.ls(basePath, settings)

  protected def listFilesWithFilter(
      basePath: String,
      branchSelector: FtpFile => Boolean,
      emitTraversedDirectories: Boolean
  ): Source[FtpFile, NotUsed] =
    Ftps.ls(basePath, settings, branchSelector, emitTraversedDirectories)

  protected def retrieveFromPath(
      path: String,
      fromRoot: Boolean = false
  ): Source[ByteString, Future[IOResult]] =
    Ftps.fromPath(path, settings)

  protected def retrieveFromPathWithOffset(
      path: String,
      offset: Long
  ): Source[ByteString, Future[IOResult]] =
    Ftps.fromPath(path, settings, 8192, offset)

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]] =
    Ftps.toPath(path, settings, append)

  protected def remove(): Sink[FtpFile, Future[IOResult]] =
    Ftps.remove(settings)

  protected def move(destinationPath: FtpFile => String): Sink[FtpFile, Future[IOResult]] =
    Ftps.move(destinationPath, settings)

  protected def mkdir(basePath: String, name: String): Source[Done, NotUsed] =
    Ftps.mkdir(basePath, name, settings)
}
