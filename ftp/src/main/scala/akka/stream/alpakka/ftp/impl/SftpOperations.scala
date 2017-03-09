/*
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import akka.stream.alpakka.ftp.RemoteFileSettings.SftpSettings
import com.jcraft.jsch.{ChannelSftp, JSch}
import scala.collection.immutable
import scala.util.Try
import java.io.InputStream
import java.nio.file.Paths

private[ftp] trait SftpOperations { _: FtpLike[JSch, SftpSettings] =>

  type Handler = ChannelSftp

  def connect(connectionSettings: SftpSettings)(implicit ftpClient: JSch): Try[Handler] = Try {
    val session = ftpClient.getSession(
      connectionSettings.credentials.username,
      connectionSettings.host.getHostAddress,
      connectionSettings.port
    )
    session.setPassword(connectionSettings.credentials.password)
    val config = new java.util.Properties
    config.setProperty("StrictHostKeyChecking", if (connectionSettings.strictHostKeyChecking) "yes" else "no")
    session.setConfig(config)
    session.connect()
    val channel = session.openChannel("sftp").asInstanceOf[ChannelSftp]
    channel.connect()
    channel
  }

  def disconnect(handler: Handler)(implicit ftpClient: JSch): Unit = {
    val session = handler.getSession
    if (session.isConnected) {
      session.disconnect()
    }
    if (handler.isConnected) {
      handler.disconnect()
    }
  }

  def listFiles(basePath: String, handler: Handler): immutable.Seq[FtpFile] = {
    val path = if (!basePath.isEmpty && basePath.head != '/') s"/$basePath" else basePath
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val entries = handler.ls(path).toSeq.filter {
      case entry: Handler#LsEntry => entry.getFilename != "." && entry.getFilename != ".."
    } // TODO
    entries.map {
      case entry: Handler#LsEntry =>
        FtpFile(entry.getFilename, Paths.get(s"$path/${entry.getFilename}").normalize.toString, entry.getAttrs.isDir)
    }.toVector
  }

  def listFiles(handler: Handler): immutable.Seq[FtpFile] = listFiles(".", handler) // TODO

  def retrieveFileInputStream(name: String, handler: Handler): Try[InputStream] = Try {
    handler.get(name)
  }
}
