/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import org.apache.commons.net.ftp.{FTP, FTPClient}
import scala.collection.immutable
import scala.util.Try
import java.io.{IOException, InputStream}
import java.nio.file.Paths

private[ftp] trait FtpOperations { _: FtpLike[FTPClient, FtpFileSettings] =>

  type Handler = FTPClient

  def connect(connectionSettings: FtpFileSettings)(implicit ftpClient: FTPClient): Try[Handler] = Try {
    ftpClient.connect(connectionSettings.host, connectionSettings.port)
    ftpClient.login(
      connectionSettings.credentials.username,
      connectionSettings.credentials.password
    )
    if (connectionSettings.binary) {
      ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
    }
    if (connectionSettings.passiveMode) {
      ftpClient.enterLocalPassiveMode()
    }
    ftpClient
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
