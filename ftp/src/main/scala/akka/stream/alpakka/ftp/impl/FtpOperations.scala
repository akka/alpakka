/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import org.apache.commons.net.ftp.{ FTP, FTPClient }
import scala.collection.immutable
import scala.util.Try
import java.io.{ IOException, InputStream }
import java.nio.file.Paths

private[ftp] trait FtpOperations { _: FtpLike[FTPClient] =>

  type Handler = FTPClient

  def connect(
      connectionSettings: RemoteFileSettings
  )(implicit ftpClient: FTPClient): Try[Handler] = Try {
    connectionSettings match {
      case settings: FtpFileSettings =>
        ftpClient.connect(settings.host, settings.port)
        ftpClient.login(
          settings.credentials.username,
          settings.credentials.password
        )
        if (settings.binary) {
          ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
        }
        if (settings.passiveMode) {
          ftpClient.enterLocalPassiveMode()
        }
        ftpClient
      case _ => throw new IllegalArgumentException(s"Invalid configuration: $connectionSettings")
    }
  }

  def disconnect(handler: Handler)(implicit ftpClient: FTPClient): Unit =
    if (ftpClient.isConnected) {
      ftpClient.logout()
      ftpClient.disconnect()
    }

  def listFiles(basePath: String, handler: Handler): immutable.Seq[FtpFile] = {
    val path = if (!basePath.isEmpty && basePath.head != '/') s"/$basePath" else basePath
    handler
      .listFiles(path)
      .map { file =>
        FtpFile(file.getName, Paths.get(s"$path/${file.getName}").normalize.toString, file.isDirectory)
      }
      .toVector
  }

  def listFiles(handler: Handler): immutable.Seq[FtpFile] = listFiles("", handler)

  def retrieveFileInputStream(name: String, handler: Handler): Try[InputStream] = Try {
    val is = handler.retrieveFileStream(name)
    if (is != null) is else throw new IOException(s"$name: No such file or directory")
  }
}
