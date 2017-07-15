/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp

import akka.NotUsed
import akka.stream.alpakka.ftp.RemoteFileSettings.FtpsSettings
import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
import akka.stream.alpakka.ftp.scaladsl.Ftps
import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import java.net.InetAddress

trait BaseFtpsSpec extends FtpsSupportImpl with BaseSpec {

  //#create-settings
  val settings = FtpsSettings(
    InetAddress.getByName("localhost"),
    getPort,
    AnonFtpCredentials,
    binary = true,
    passiveMode = true
  )
  //#create-settings

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Ftps.ls(basePath, settings)

  protected def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Ftps.fromPath(path, settings)

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]] =
    Ftps.toPath(path, settings, append)

}
