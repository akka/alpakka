/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp

import akka.NotUsed
import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
import akka.stream.alpakka.ftp.scaladsl.Sftp
import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import java.net.InetAddress

trait BaseSftpSpec extends SftpSupportImpl with BaseSpec {

  //#create-settings
  val settings = SftpSettings(
    InetAddress.getByName("localhost"),
    getPort,
    AnonFtpCredentials,
    strictHostKeyChecking = false,
    knownHosts = None,
    sftpIdentity = None,
    options = Map.empty
  )
  //#create-settings

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Sftp.ls(basePath, settings)

  protected def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Sftp.fromPath(path, settings)

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]] =
    Sftp.toPath(path, settings, append)
}
