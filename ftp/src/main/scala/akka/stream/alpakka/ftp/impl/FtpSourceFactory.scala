/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp.impl

import akka.stream.alpakka.ftp.FtpCredentials.{ AnonFtpCredentials, NonAnonFtpCredentials }
import akka.stream.alpakka.ftp.RemoteFileSettings
import akka.stream.alpakka.ftp.RemoteFileSettings._
import com.jcraft.jsch.JSch
import org.apache.commons.net.ftp.FTPClient
import java.net.InetAddress
import java.nio.file.Path

private[ftp] trait FtpSourceFactory[FtpClient] { self =>

  protected[this] final val DefaultChunkSize = 8192

  protected[this] def ftpClient: FtpClient

  protected[this] def ftpBrowserSourceName: String

  protected[this] def ftpIOSourceName: String

  protected[this] def createBrowserGraph(
      _sourceName: String,
      _basePath: String,
      _connectionSettings: RemoteFileSettings
  )(implicit _ftpLike: FtpLike[FtpClient]): FtpBrowserGraphStage[FtpClient] =
    new FtpBrowserGraphStage[FtpClient] {
      val name: String = _sourceName
      val basePath: String = _basePath
      val connectionSettings: RemoteFileSettings = _connectionSettings
      val ftpClient: FtpClient = self.ftpClient
      val ftpLike: FtpLike[FtpClient] = _ftpLike
    }

  protected[this] def createIOGraph(
      _sourceName: String,
      _path: Path,
      _connectionSettings: RemoteFileSettings,
      _chunkSize: Int
  )(implicit _ftpLike: FtpLike[FtpClient]): FtpIOGraphStage[FtpClient] =
    new FtpIOGraphStage[FtpClient] {
      val name: String = _sourceName
      val path: Path = _path
      val connectionSettings: RemoteFileSettings = _connectionSettings
      val ftpClient: FtpClient = self.ftpClient
      val ftpLike: FtpLike[FtpClient] = _ftpLike
      val chunkSize: Int = _chunkSize
    }

  protected[this] def defaultSettings(
      hostname: String,
      username: Option[String] = None,
      password: Option[String] = None
  ): RemoteFileSettings
}

private[ftp] trait FtpSource { _: FtpSourceFactory[_] =>
  protected final val FtpBrowserSourceName = "FtpBrowserSource"
  protected final val FtpIOSourceName = "FtpIOSource"
  protected def ftpClient: FTPClient = new FTPClient
  protected val ftpBrowserSourceName: String = FtpBrowserSourceName
  protected val ftpIOSourceName: String = FtpIOSourceName
}

private[ftp] trait FtpsSource { _: FtpSourceFactory[_] =>
  protected final val FtpsBrowserSourceName = "FtpsBrowserSource"
  protected final val FtpsIOSourceName = "FtpsIOSource"
  protected def ftpClient: FTPClient = new FTPClient
  protected val ftpBrowserSourceName: String = FtpsBrowserSourceName
  protected val ftpIOSourceName: String = FtpsIOSourceName
}

private[ftp] trait SftpSource { _: FtpSourceFactory[_] =>
  protected final val sFtpBrowserSourceName = "sFtpBrowserSource"
  protected final val sFtpIOSourceName = "sFtpIOSource"
  protected def ftpClient: JSch = new JSch
  protected val ftpBrowserSourceName: String = sFtpBrowserSourceName
  protected val ftpIOSourceName: String = sFtpIOSourceName
}

private[ftp] trait FtpDefaultSettings {
  protected def defaultSettings(
      hostname: String,
      username: Option[String],
      password: Option[String]
  ): RemoteFileSettings =
    FtpSettings(
      InetAddress.getByName(hostname),
      DefaultFtpPort,
      if (username.isDefined)
        NonAnonFtpCredentials(username.get, password.getOrElse(""))
      else
        AnonFtpCredentials
    )
}

private[ftp] trait FtpsDefaultSettings {
  protected def defaultSettings(
      hostname: String,
      username: Option[String],
      password: Option[String]
  ): RemoteFileSettings =
    FtpsSettings(
      InetAddress.getByName(hostname),
      DefaultFtpsPort,
      if (username.isDefined)
        NonAnonFtpCredentials(username.get, password.getOrElse(""))
      else
        AnonFtpCredentials
    )
}

private[ftp] trait SftpDefaultSettings {
  protected def defaultSettings(
      hostname: String,
      username: Option[String],
      password: Option[String]
  ): RemoteFileSettings =
    SftpSettings(
      InetAddress.getByName(hostname),
      DefaultSftpPort,
      if (username.isDefined)
        NonAnonFtpCredentials(username.get, password.getOrElse(""))
      else
        AnonFtpCredentials
    )
}
