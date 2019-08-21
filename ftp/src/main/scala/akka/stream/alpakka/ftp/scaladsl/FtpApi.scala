/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.scaladsl

import akka.{Done, NotUsed}
import akka.stream.{IOResult, Materializer}
import akka.stream.alpakka.ftp.impl._
import akka.stream.alpakka.ftp.{FtpFile, FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

import scala.concurrent.Future

sealed trait FtpApi[FtpClient, S <: RemoteFileSettings] { _: FtpSourceFactory[FtpClient, S] =>

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s from the remote user `root` directory.
   * By default, `anonymous` credentials will be used.
   *
   * @param host FTP, FTPs or SFTP host
   * @return A [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s
   */
  def ls(host: String): Source[FtpFile, NotUsed] =
    ls(host, basePath = "")

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s from a base path.
   * By default, `anonymous` credentials will be used.
   *
   * @param host FTP, FTPs or SFTP host
   * @param basePath Base path from which traverse the remote file server
   * @return A [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s
   */
  def ls(host: String, basePath: String): Source[FtpFile, NotUsed] =
    ls(basePath, defaultSettings(host))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s from the remote user `root` directory.
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @return A [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s
   */
  def ls(host: String, username: String, password: String): Source[FtpFile, NotUsed] =
    ls("", defaultSettings(host, Some(username), Some(password)))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s from a base path.
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @param basePath Base path from which traverse the remote file server
   * @return A [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s
   */
  def ls(host: String, username: String, password: String, basePath: String): Source[FtpFile, NotUsed] =
    ls(basePath, defaultSettings(host, Some(username), Some(password)))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s from a base path.
   *
   * @param basePath Base path from which traverse the remote file server
   * @param connectionSettings connection settings
   * @return A [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s
   */
  def ls(basePath: String, connectionSettings: S): Source[FtpFile, NotUsed] =
    ls(basePath, connectionSettings, f => true)

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s from a base path.
   *
   * @param basePath Base path from which traverse the remote file server
   * @param connectionSettings connection settings
   * @param branchSelector a function for pruning the tree. Takes a remote folder and return true
   *                       if you want to enter that remote folder.
   *                       Default behaviour is fully recursive which is equivalent with calling this function
   *                       with [ls(basePath,connectionSettings,f=>true)].
   *
   *                       Calling [ls(basePath,connectionSettings,f=>false)] will emit only the files and folder in
   *                       non-recursive fashion
   *
   * @return A [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s
   */
  def ls(basePath: String, connectionSettings: S, branchSelector: FtpFile => Boolean): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(basePath, connectionSettings, branchSelector, _emitTraversedDirectories = false)
    )

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s from a base path.
   *
   * @param basePath Base path from which traverse the remote file server
   * @param connectionSettings connection settings
   * @param branchSelector a function for pruning the tree. Takes a remote folder and return true
   *                       if you want to enter that remote folder.
   *                       Default behaviour is fully recursive which is equivalent with calling this function
   *                       with [ls(basePath,connectionSettings,f=>true)].
   *
   *                       Calling [ls(basePath,connectionSettings,f=>false)] will emit only the files and folder in
   *                       non-recursive fashion
   * @param emitTraversedDirectories whether to include entered directories in the stream
   *
   * @return A [[akka.stream.scaladsl.Source Source]] of [[FtpFile]]s
   */
  def ls(basePath: String,
         connectionSettings: S,
         branchSelector: FtpFile => Boolean,
         emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Source.fromGraph(createBrowserGraph(basePath, connectionSettings, branchSelector, emitTraversedDirectories))

  /**
   *  Scala API for creating a directory in a given path
   * @param basePath path to start with
   * @param name name of a directory to create
   * @param connectionSettings connection settings
   * @return [[akka.stream.scaladsl.Source Source]] of [[akka.Done]]
   */
  def mkdir(basePath: String, name: String, connectionSettings: S): Source[Done, NotUsed] =
    Source.fromGraph(createMkdirGraph(basePath, name, connectionSettings)).map(_ => Done)

  /**
   *  Scala API for creating a directory in a given path
   * @param basePath path to start with
   * @param name name of a directory to create
   * @param connectionSettings connection settings
   * @return [[scala.concurrent.Future Future]] of [[akka.Done]] indicating a materialized, asynchronous request
   */
  def mkdirAsync(basePath: String, name: String, connectionSettings: S)(implicit mat: Materializer): Future[Done] =
    mkdir(basePath, name, connectionSettings).runWith(Sink.head)

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param host FTP, FTPs or SFTP host
   * @param path the file path
   * @return A [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[scala.concurrent.Future Future]] of [[IOResult]]
   */
  def fromPath(host: String, path: String): Source[ByteString, Future[IOResult]] =
    fromPath(path, defaultSettings(host))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @param path the file path
   * @return A [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[scala.concurrent.Future Future]] of [[IOResult]]
   */
  def fromPath(host: String, username: String, password: String, path: String): Source[ByteString, Future[IOResult]] =
    fromPath(path, defaultSettings(host, Some(username), Some(password)))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param path the file path
   * @param connectionSettings connection settings
   * @param chunkSize the size of transmitted [[akka.util.ByteString ByteString]] chunks
   * @return A [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[scala.concurrent.Future Future]] of [[IOResult]]
   */
  def fromPath(
      path: String,
      connectionSettings: S,
      chunkSize: Int = DefaultChunkSize
  ): Source[ByteString, Future[IOResult]] =
    fromPath(path, connectionSettings, chunkSize, 0L)

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param path the file path
   * @param connectionSettings connection setting
   * @param chunkSize the size of transmitted [[akka.util.ByteString ByteString]] chunks
   * @param offset the offset into the remote file at which to start the file transfer
   * @return A [[akka.stream.scaladsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[scala.concurrent.Future Future]] of [[IOResult]]
   */
  def fromPath(
      path: String,
      connectionSettings: S,
      chunkSize: Int,
      offset: Long
  ): Source[ByteString, Future[IOResult]] =
    Source.fromGraph(createIOSource(path, connectionSettings, chunkSize, offset))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Sink Sink]] of [[akka.util.ByteString ByteString]] to some file path.
   *
   * @param path the file path
   * @param connectionSettings connection settings
   * @param append append data if a file already exists, overwrite the file if not
   * @return A [[akka.stream.scaladsl.Sink Sink]] of [[akka.util.ByteString ByteString]] that materializes to a [[scala.concurrent.Future Future]] of [[IOResult]]
   */
  def toPath(
      path: String,
      connectionSettings: S,
      append: Boolean = false
  ): Sink[ByteString, Future[IOResult]] =
    Sink.fromGraph(createIOSink(path, connectionSettings, append))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Sink Sink]] of a [[FtpFile]] that moves a file to some file path.
   *
   * @param destinationPath a function that returns path to where the [[FtpFile]] is moved.
   * @param connectionSettings connection settings
   * @return A [[akka.stream.scaladsl.Sink Sink]] of [[FtpFile]] that materializes to a [[scala.concurrent.Future Future]] of [[IOResult]]
   */
  def move(destinationPath: FtpFile => String, connectionSettings: S): Sink[FtpFile, Future[IOResult]] =
    Sink.fromGraph(createMoveSink(destinationPath, connectionSettings))

  /**
   * Scala API: creates a [[akka.stream.scaladsl.Sink Sink]] of a [[FtpFile]] that removes a file.
   *
   * @param connectionSettings connection settings
   * @return A [[akka.stream.scaladsl.Sink Sink]] of [[FtpFile]] that materializes to a [[scala.concurrent.Future Future]] of [[IOResult]]
   */
  def remove(connectionSettings: S): Sink[FtpFile, Future[IOResult]] =
    Sink.fromGraph(createRemoveSink(connectionSettings))

  protected[this] implicit def ftpLike: FtpLike[FtpClient, S]
}

object Ftp extends FtpApi[FTPClient, FtpSettings] with FtpSourceParams
object Ftps extends FtpApi[FTPSClient, FtpsSettings] with FtpsSourceParams

class SftpApi extends FtpApi[SSHClient, SftpSettings] with SftpSourceParams
object Sftp extends SftpApi {

  /**
   * Scala API: creates a [[akka.stream.alpakka.ftp.scaladsl.SftpApi]]
   *
   * @param customSshClient custom ssh client
   * @return A [[akka.stream.alpakka.ftp.scaladsl.SftpApi]]
   */
  def apply(customSshClient: SSHClient): SftpApi =
    new SftpApi {
      override val sshClient = customSshClient
    }
}
