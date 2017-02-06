/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.stream.alpakka.ftp.RemoteFileSettings.FtpSettings
import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.util.ByteString
import scala.concurrent.Future
import java.net.InetAddress

trait BaseFtpSpec extends PlainFtpSupportImpl with BaseSpec {

  //#create-settings
  val settings = FtpSettings(
    InetAddress.getByName("localhost"),
    getPort,
    AnonFtpCredentials,
    binary = true,
    passiveMode = true
  )
  //#create-settings

  //#traversing
  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Ftp.ls(basePath, settings)
  //#traversing

  //#retrieving
  protected def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Ftp.fromPath(getFileSystem.getPath(path), settings)
  //#retrieving
}
