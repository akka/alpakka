/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.impl

import java.net.InetAddress

import akka.annotation.InternalApi
import akka.stream.alpakka.ftp.FtpCredentials
import akka.stream.alpakka.ftp._
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpSourceFactory[FtpClient] { self =>

  type S <: RemoteFileSettings

  protected[this] final val DefaultChunkSize = 8192

  protected[this] def ftpClient: () => FtpClient

  protected[this] def ftpBrowserSourceName: String

  protected[this] def ftpIOSourceName: String

  protected[this] def ftpIOSinkName: String

  protected[this] def createBrowserGraph(
      _basePath: String,
      _connectionSettings: S,
      _branchSelector: FtpFile => Boolean,
      _emitTraversedDirectories: Boolean
  )(implicit _ftpLike: FtpLike[FtpClient, S]): FtpBrowserGraphStage[FtpClient, S] =
    new FtpBrowserGraphStage[FtpClient, S] {
      lazy val name: String = ftpBrowserSourceName
      val basePath: String = _basePath
      val connectionSettings: S = _connectionSettings
      val ftpClient: () => FtpClient = self.ftpClient
      val ftpLike: FtpLike[FtpClient, S] = _ftpLike
      override val branchSelector: (FtpFile) => Boolean = _branchSelector
      val emitTraversedDirectories: Boolean = _emitTraversedDirectories
    }

  protected[this] def createIOSource(
      _path: String,
      _connectionSettings: S,
      _chunkSize: Int
  )(implicit _ftpLike: FtpLike[FtpClient, S]): FtpIOSourceStage[FtpClient, S] =
    new FtpIOSourceStage[FtpClient, S] {
      lazy val name: String = ftpIOSourceName
      val path: String = _path
      val connectionSettings: S = _connectionSettings
      val ftpClient: () => FtpClient = self.ftpClient
      val ftpLike: FtpLike[FtpClient, S] = _ftpLike
      val chunkSize: Int = _chunkSize
    }

  protected[this] def createIOSink(
      _path: String,
      _connectionSettings: S,
      _append: Boolean
  )(implicit _ftpLike: FtpLike[FtpClient, S]): FtpIOSinkStage[FtpClient, S] =
    new FtpIOSinkStage[FtpClient, S] {
      lazy val name: String = ftpIOSinkName
      val path: String = _path
      val connectionSettings: S = _connectionSettings
      val ftpClient: () => FtpClient = self.ftpClient
      val ftpLike: FtpLike[FtpClient, S] = _ftpLike
      val append: Boolean = _append
    }

  protected[this] def createMoveSink(
      _destinationPath: FtpFile => String,
      _connectionSettings: S
  )(implicit _ftpLike: FtpLike[FtpClient, S]) =
    new FtpMoveSink[FtpClient, S] {
      val connectionSettings: S = _connectionSettings
      val ftpClient: () => FtpClient = self.ftpClient
      val ftpLike: FtpLike[FtpClient, S] = _ftpLike
      val destinationPath: FtpFile => String = _destinationPath
    }

  protected[this] def createRemoveSink(
      _connectionSettings: S
  )(implicit _ftpLike: FtpLike[FtpClient, S]) =
    new FtpRemoveSink[FtpClient, S] {
      val connectionSettings: S = _connectionSettings
      val ftpClient: () => FtpClient = self.ftpClient
      val ftpLike: FtpLike[FtpClient, S] = _ftpLike
    }

  protected[this] def defaultSettings(
      hostname: String,
      username: Option[String] = None,
      password: Option[String] = None
  ): S
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpSource extends FtpSourceFactory[FTPClient] {
  protected final val FtpBrowserSourceName = "FtpBrowserSource"
  protected final val FtpIOSourceName = "FtpIOSource"
  protected final val FtpIOSinkName = "FtpIOSink"
  protected val ftpClient: () => FTPClient = () => new FTPClient
  protected val ftpBrowserSourceName: String = FtpBrowserSourceName
  protected val ftpIOSourceName: String = FtpIOSourceName
  protected val ftpIOSinkName: String = FtpIOSinkName
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpsSource extends FtpSourceFactory[FTPSClient] {
  protected final val FtpsBrowserSourceName = "FtpsBrowserSource"
  protected final val FtpsIOSourceName = "FtpsIOSource"
  protected final val FtpsIOSinkName = "FtpsIOSink"
  protected val ftpClient: () => FTPSClient = () => new FTPSClient
  protected val ftpBrowserSourceName: String = FtpsBrowserSourceName
  protected val ftpIOSourceName: String = FtpsIOSourceName
  protected val ftpIOSinkName: String = FtpsIOSinkName
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait SftpSource extends FtpSourceFactory[SSHClient] {
  protected final val sFtpBrowserSourceName = "sFtpBrowserSource"
  protected final val sFtpIOSourceName = "sFtpIOSource"
  protected final val sFtpIOSinkName = "sFtpIOSink"
  def sshClient(): SSHClient = new SSHClient()
  protected val ftpClient: () => SSHClient = () => sshClient()
  protected val ftpBrowserSourceName: String = sFtpBrowserSourceName
  protected val ftpIOSourceName: String = sFtpIOSourceName
  protected val ftpIOSinkName: String = sFtpIOSinkName
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpDefaultSettings {
  protected def defaultSettings(
      hostname: String,
      username: Option[String],
      password: Option[String]
  ): FtpSettings =
    FtpSettings(
      InetAddress.getByName(hostname)
    ).withCredentials(
      if (username.isDefined)
        FtpCredentials.create(username.get, password.getOrElse(""))
      else
        FtpCredentials.anonymous
    )
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpsDefaultSettings {
  protected def defaultSettings(
      hostname: String,
      username: Option[String],
      password: Option[String]
  ): FtpsSettings =
    FtpsSettings(
      InetAddress.getByName(hostname)
    ).withCredentials(
      if (username.isDefined)
        FtpCredentials.create(username.get, password.getOrElse(""))
      else
        FtpCredentials.anonymous
    )
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait SftpDefaultSettings {
  protected def defaultSettings(
      hostname: String,
      username: Option[String],
      password: Option[String]
  ): SftpSettings =
    SftpSettings(
      InetAddress.getByName(hostname)
    ).withCredentials(
      if (username.isDefined)
        FtpCredentials.create(username.get, password.getOrElse(""))
      else
        FtpCredentials.anonymous
    )
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpSourceParams extends FtpSource with FtpDefaultSettings {
  type S = FtpSettings
  protected[this] val ftpLike: FtpLike[FTPClient, S] = FtpLike.ftpLikeInstance
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait FtpsSourceParams extends FtpsSource with FtpsDefaultSettings {
  type S = FtpsSettings
  protected[this] val ftpLike: FtpLike[FTPSClient, S] = FtpLike.ftpsLikeInstance
}

/**
 * INTERNAL API
 */
@InternalApi
private[ftp] trait SftpSourceParams extends SftpSource with SftpDefaultSettings {
  type S = SftpSettings
  protected[this] val ftpLike: FtpLike[SSHClient, S] = FtpLike.sFtpLikeInstance
}
