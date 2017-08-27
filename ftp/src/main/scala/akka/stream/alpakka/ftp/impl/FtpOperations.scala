/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import org.apache.commons.net.ftp.{FTP, FTPClient, FTPFile}
import scala.collection.immutable
import scala.util.Try
import java.io.{IOException, InputStream, OutputStream}
import java.nio.file.Paths
import java.nio.file.attribute.PosixFilePermission

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
      .collect {
        case file: FTPFile if file.getName != "." && file.getName != ".." =>
          FtpFile(
            file.getName,
            Paths.get(s"$path/${file.getName}").normalize.toString,
            file.isDirectory,
            file.getSize,
            file.getTimestamp.getTimeInMillis,
            getPosixFilePermissions(file)
          )
      }
      .toVector
  }

  private def getPosixFilePermissions(file: FTPFile) =
    Map(
      PosixFilePermission.OWNER_READ → file.hasPermission(FTPFile.USER_ACCESS, FTPFile.READ_PERMISSION),
      PosixFilePermission.OWNER_WRITE → file.hasPermission(FTPFile.USER_ACCESS, FTPFile.WRITE_PERMISSION),
      PosixFilePermission.OWNER_EXECUTE → file.hasPermission(FTPFile.USER_ACCESS, FTPFile.EXECUTE_PERMISSION),
      PosixFilePermission.GROUP_READ → file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.READ_PERMISSION),
      PosixFilePermission.GROUP_WRITE → file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.WRITE_PERMISSION),
      PosixFilePermission.GROUP_EXECUTE → file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.EXECUTE_PERMISSION),
      PosixFilePermission.OTHERS_READ → file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.READ_PERMISSION),
      PosixFilePermission.OTHERS_WRITE → file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.WRITE_PERMISSION),
      PosixFilePermission.OTHERS_EXECUTE → file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.EXECUTE_PERMISSION)
    ).collect {
      case (perm, true) ⇒ perm
    }.toSet

  def listFiles(handler: Handler): immutable.Seq[FtpFile] = listFiles("", handler)

  def retrieveFileInputStream(name: String, handler: Handler): Try[InputStream] = Try {
    val is = handler.retrieveFileStream(name)
    if (is != null) is else throw new IOException(s"$name: No such file or directory")
  }

  def storeFileOutputStream(name: String, handler: Handler, append: Boolean): Try[OutputStream] = Try {
    val os = if (append) handler.appendFileStream(name) else handler.storeFileStream(name)
    if (os != null) os else throw new IOException(s"Could not write to $name")
  }
}
