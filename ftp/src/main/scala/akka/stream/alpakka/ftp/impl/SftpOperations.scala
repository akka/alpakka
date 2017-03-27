/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp
package impl

import com.jcraft.jsch.{ChannelSftp, JSch}

import scala.collection.immutable
import scala.util.Try
import scala.collection.JavaConverters._
import java.io.{InputStream, OutputStream}
import java.nio.file.Paths
import scala.collection.JavaConversions._
import java.nio.file.attribute.PosixFilePermissions

import com.jcraft.jsch.ChannelSftp.{APPEND, OVERWRITE}

private[ftp] trait SftpOperations { _: FtpLike[JSch, SftpSettings] =>

  type Handler = ChannelSftp

  private def configureIdentity(sftpIdentity: SftpIdentity)(implicit ftpClient: JSch) = sftpIdentity match {
    case identity: RawKeySftpIdentity =>
      ftpClient.addIdentity(identity.name, identity.privateKey, identity.publicKey.orNull, identity.password.orNull)
    case identity: KeyFileSftpIdentity =>
      ftpClient.addIdentity(identity.privateKey, identity.publicKey.orNull, identity.password.orNull)
  }

  def connect(connectionSettings: SftpSettings)(implicit ftpClient: JSch): Try[Handler] = Try {
    connectionSettings.sftpIdentity.foreach(configureIdentity)
    connectionSettings.knownHosts.foreach(ftpClient.setKnownHosts)
    val session = ftpClient.getSession(
      connectionSettings.credentials.username,
      connectionSettings.host.getHostAddress,
      connectionSettings.port
    )
    session.setPassword(connectionSettings.credentials.password)
    val config = new java.util.Properties
    config.setProperty("StrictHostKeyChecking", if (connectionSettings.strictHostKeyChecking) "yes" else "no")
    config.putAll(connectionSettings.options)
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
        FtpFile(
          entry.getFilename,
          Paths.get(s"$path/${entry.getFilename}").normalize.toString,
          entry.getAttrs.isDir,
          entry.getAttrs.getSize,
          entry.getAttrs.getMTime * 1000L,
          getPosixFilePermissions(entry.getAttrs.getPermissionsString)
        )
    }.toVector
  }

  private def getPosixFilePermissions(permissions: String) =
    PosixFilePermissions
      .fromString(
        permissions.replace('s', '-').drop(1)
      )
      .asScala
      .toSet

  def listFiles(handler: Handler): immutable.Seq[FtpFile] = listFiles(".", handler) // TODO

  def retrieveFileInputStream(name: String, handler: Handler): Try[InputStream] = Try {
    handler.get(name)
  }

  def storeFileOutputStream(name: String, handler: Handler, append: Boolean): Try[OutputStream] = Try {
    handler.put(name, if (append) APPEND else OVERWRITE)
  }
}
