/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp.scaladsl

import akka.NotUsed
import akka.stream.alpakka.ftp.impl._
import akka.stream.IOResult
import akka.stream.alpakka.ftp.{ FtpFile, RemoteFileSettings }
import akka.stream.alpakka.ftp.impl.{ FtpLike, FtpSourceFactory }
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.jcraft.jsch.JSch
import org.apache.commons.net.ftp.FTPClient
import scala.concurrent.Future
import java.nio.file.Path

trait FtpApi[FtpClient] { _: FtpSourceFactory[FtpClient] =>

  /**
   * Scala API: creates a [[Source]] of [[FtpFile]]s from the remote user `root` directory.
   * By default, `anonymous` credentials will be used.
   *
   * @param host FTP, FTPs or SFTP host
   * @param ftpLike Implicitly resolved typeclass instance for FTP, FTPs or SFTP operations
   * @return A [[Source]] of [[FtpFile]]s
   */
  def ls(host: String)(implicit ftpLike: FtpLike[FtpClient]): Source[FtpFile, NotUsed] =
    ls(host, basePath = "")

  /**
   * Scala API: creates a [[Source]] of [[FtpFile]]s from a base path.
   * By default, `anonymous` credentials will be used.
   *
   * @param host FTP, FTPs or SFTP host
   * @param basePath Base path from which traverse the remote file server
   * @param ftpLike Implicitly resolved typeclass instance for FTP, FTPs or SFTP operations
   * @return A [[Source]] of [[FtpFile]]s
   */
  def ls(
      host: String,
      basePath: String
  )(implicit ftpLike: FtpLike[FtpClient]): Source[FtpFile, NotUsed] =
    ls(basePath, defaultSettings(host))

  /**
   * Scala API: creates a [[Source]] of [[FtpFile]]s from the remote user `root` directory.
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @param ftpLike Implicitly resolved typeclass instance for FTP, FTPs or SFTP operations
   * @return A [[Source]] of [[FtpFile]]s
   */
  def ls(
      host: String,
      username: String,
      password: String
  )(implicit ftpLike: FtpLike[FtpClient]): Source[FtpFile, NotUsed] =
    ls("", defaultSettings(host, Some(username), Some(password)))

  /**
   * Scala API: creates a [[Source]] of [[FtpFile]]s from a base path.
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @param basePath Base path from which traverse the remote file server
   * @param ftpLike Implicitly resolved typeclass instance for FTP, FTPs or SFTP operations
   * @return A [[Source]] of [[FtpFile]]s
   */
  def ls(
      host: String,
      username: String,
      password: String,
      basePath: String
  )(implicit ftpLike: FtpLike[FtpClient]): Source[FtpFile, NotUsed] =
    ls(basePath, defaultSettings(host, Some(username), Some(password)))

  /**
   * Scala API: creates a [[Source]] of [[FtpFile]]s from a base path.
   *
   * @param basePath Base path from which traverse the remote file server
   * @param connectionSettings connection settings
   * @param ftpLike Implicitly resolved typeclass instance for FTP, FTPs or SFTP operations
   * @return A [[Source]] of [[FtpFile]]s
   */
  def ls(
      basePath: String,
      connectionSettings: RemoteFileSettings
  )(implicit ftpLike: FtpLike[FtpClient]): Source[FtpFile, NotUsed] =
    Source.fromGraph(createBrowserGraph(ftpBrowserSourceName, basePath, connectionSettings))

  /**
   * Scala API: creates a [[Source]] of [[ByteString]] from some file [[Path]].
   *
   * @param host FTP, FTPs or SFTP host
   * @param path the file path
   * @param ftpLike Implicitly resolved typeclass instance for FTP, FTPs or SFTP operations
   * @return A [[Source]] of [[ByteString]] that materializes to a [[Future]] of [[IOResult]]
   */
  def fromPath(
      host: String,
      path: Path
  )(implicit ftpLike: FtpLike[FtpClient]): Source[ByteString, Future[IOResult]] =
    fromPath(path, defaultSettings(host))

  /**
   * Scala API: creates a [[Source]] of [[ByteString]] from some file [[Path]].
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @param path the file path
   * @param ftpLike Implicitly resolved typeclass instance for FTP, FTPs or SFTP operations
   * @return A [[Source]] of [[ByteString]] that materializes to a [[Future]] of [[IOResult]]
   */
  def fromPath(
      host: String,
      username: String,
      password: String,
      path: Path
  )(implicit ftpLike: FtpLike[FtpClient]): Source[ByteString, Future[IOResult]] =
    fromPath(path, defaultSettings(host, Some(username), Some(password)))

  /**
   * Scala API: creates a [[Source]] of [[ByteString]] from some file [[Path]].
   *
   * @param path the file path
   * @param connectionSettings connection settings
   * @param chunkSize the size of transmitted [[ByteString]] chunks
   * @param ftpLike Implicitly resolved typeclass instance for FTP, FTPs or SFTP operations
   * @return A [[Source]] of [[ByteString]] that materializes to a [[Future]] of [[IOResult]]
   */
  def fromPath(
      path: Path,
      connectionSettings: RemoteFileSettings,
      chunkSize: Int = DefaultChunkSize
  )(implicit ftpLike: FtpLike[FtpClient]): Source[ByteString, Future[IOResult]] =
    Source.fromGraph(createIOGraph(ftpIOSourceName, path, connectionSettings, chunkSize))
}

object Ftp extends FtpApi[FTPClient] with FtpSourceFactory[FTPClient] with FtpSource with FtpDefaultSettings
object Ftps extends FtpApi[FTPClient] with FtpSourceFactory[FTPClient] with FtpsSource with FtpsDefaultSettings
object sFtp extends FtpApi[JSch] with FtpSourceFactory[JSch] with SftpSource with SftpDefaultSettings
