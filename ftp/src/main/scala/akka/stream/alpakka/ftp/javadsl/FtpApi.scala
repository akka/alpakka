/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp.javadsl

import java.util.concurrent.CompletionStage
import java.util.function._

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.alpakka.ftp._
import akka.stream.alpakka.ftp.impl._
import akka.stream.javadsl.{Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

import scala.compat.java8.FunctionConverters._

sealed trait FtpApi[FtpClient, S <: RemoteFileSettings] { _: FtpSourceFactory[FtpClient, S] =>

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s from the remote user `root` directory.
   * By default, `anonymous` credentials will be used.
   *
   * @param host FTP, FTPs or SFTP host
   * @return A [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s
   */
  def ls(host: String): Source[FtpFile, NotUsed]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s from a base path.
   * By default, `anonymous` credentials will be used.
   *
   * @param host FTP, FTPs or SFTP host
   * @param basePath Base path from which traverse the remote file server
   * @return A [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s
   */
  def ls(
      host: String,
      basePath: String
  ): Source[FtpFile, NotUsed]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s from the remote user `root` directory.
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @return A [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s
   */
  def ls(
      host: String,
      username: String,
      password: String
  ): Source[FtpFile, NotUsed]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s from a base path.
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @param basePath Base path from which traverse the remote file server
   * @return A [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s
   */
  def ls(
      host: String,
      username: String,
      password: String,
      basePath: String
  ): Source[FtpFile, NotUsed]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s from a base path.
   *
   * @param basePath Base path from which traverse the remote file server
   * @param connectionSettings connection settings
   * @return A [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s
   */
  def ls(
      basePath: String,
      connectionSettings: S
  ): Source[FtpFile, NotUsed]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s from a base path.
   *
   * @param basePath Base path from which traverse the remote file server
   * @param connectionSettings connection settings
   * @param branchSelector a predicate for pruning the tree. Takes a remote folder and return true
   *                       if you want to enter that remote folder.
   *                       Default behaviour is full recursive which is equivalent with calling this function
   *                       with [[ls(basePath,connectionSettings,f->true)]].
   *
   *                       Calling [[ls(basePath,connectionSettings,f->false)]] will emit only the files and folder in
   *                       non-recursive fashion
   *
   * @return A [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s
   */
  def ls(basePath: String, connectionSettings: S, branchSelector: Predicate[FtpFile]): Source[FtpFile, NotUsed]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s from a base path.
   *
   * @param basePath Base path from which traverse the remote file server
   * @param connectionSettings connection settings
   * @param branchSelector a predicate for pruning the tree. Takes a remote folder and return true
   *                       if you want to enter that remote folder.
   *                       Default behaviour is full recursive which is equivalent with calling this function
   *                       with [[ls(basePath,connectionSettings,f->true)]].
   *
   *                       Calling [[ls(basePath,connectionSettings,f->false)]] will emit only the files and folder in
   *                       non-recursive fashion
   * @param emitTraversedDirectories whether to include entered directories in the stream
   *
   * @return A [[akka.stream.javadsl.Source Source]] of [[FtpFile]]s
   */
  def ls(basePath: String,
         connectionSettings: S,
         branchSelector: Predicate[FtpFile],
         emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param host FTP, FTPs or SFTP host
   * @param path the file path
   * @return A [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def fromPath(
      host: String,
      path: String
  ): Source[ByteString, CompletionStage[IOResult]]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param host FTP, FTPs or SFTP host
   * @param username username
   * @param password password
   * @param path the file path
   * @return A [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def fromPath(
      host: String,
      username: String,
      password: String,
      path: String
  ): Source[ByteString, CompletionStage[IOResult]]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param path the file path
   * @param connectionSettings connection settings
   * @return A [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def fromPath(
      path: String,
      connectionSettings: S
  ): Source[ByteString, CompletionStage[IOResult]]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param path the file path
   * @param connectionSettings connection settings
   * @param chunkSize the size of transmitted [[akka.util.ByteString ByteString]] chunks
   * @return A [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def fromPath(
      path: String,
      connectionSettings: S,
      chunkSize: Int = DefaultChunkSize
  ): Source[ByteString, CompletionStage[IOResult]]

  /**
   * Java API: creates a [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] from some file path.
   *
   * @param path the file path
   * @param connectionSettings connection settings
   * @param chunkSize the size of transmitted [[akka.util.ByteString ByteString]] chunks
   * @param offset the offset into the remote file at which to start the file transfer
   * @return A [[akka.stream.javadsl.Source Source]] of [[akka.util.ByteString ByteString]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def fromPath(
      path: String,
      connectionSettings: S,
      chunkSize: Int,
      offset: Long
  ): Source[ByteString, CompletionStage[IOResult]]

  /**
   * Java API for creating a directory in a given path
   *
   * @param basePath path to start with
   * @param name name of a directory to create
   * @param connectionSettings connection settings
   * @return [[akka.stream.javadsl.Source Source]] of [[Done]]
   */
  def mkdir(basePath: String, name: String, connectionSettings: S): Source[Done, NotUsed] =
    ScalaSource.fromGraph(createMkdirGraph(basePath, name, connectionSettings)).map(_ => Done.getInstance()).asJava

  /**
   * Java API for creating a directory in a given path
   *
   * @param basePath path to start with
   * @param name name of a directory to create
   * @param connectionSettings connection settings
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of [[akka.Done]] indicating a materialized, asynchronous request
   */
  def mkdirAsync(basePath: String, name: String, connectionSettings: S, mat: Materializer): CompletionStage[Done] =
    mkdir(basePath, name, connectionSettings).runWith(Sink.head(), mat)

  /**
   * Java API: creates a [[akka.stream.javadsl.Sink Sink]] of [[akka.util.ByteString ByteString]] to some file path.
   *
   * @param path the file path
   * @param connectionSettings connection settings
   * @param append append data if a file already exists, overwrite the file if not
   * @return A [[akka.stream.javadsl.Sink Sink]] of [[akka.util.ByteString ByteString]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def toPath(
      path: String,
      connectionSettings: S,
      append: Boolean
  ): Sink[ByteString, CompletionStage[IOResult]]

  /**
   * Java API: creates a [[akka.stream.javadsl.Sink Sink]] of [[akka.util.ByteString ByteString]] to some file path.
   * If a file already exists at the specified target path, it will get overwritten.
   *
   * @param path the file path
   * @param connectionSettings connection settings
   * @return A [[akka.stream.javadsl.Sink Sink]] of [[akka.util.ByteString ByteString]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def toPath(
      path: String,
      connectionSettings: S
  ): Sink[ByteString, CompletionStage[IOResult]]

  /**
   * Java API: creates a [[akka.stream.javadsl.Sink Sink]] of a [[FtpFile]] that moves a file to some file path.
   *
   * @param destinationPath a function that returns path to where the [[FtpFile]] is moved.
   * @param connectionSettings connection settings
   * @return A [[akka.stream.javadsl.Sink Sink]] of [[FtpFile]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def move(destinationPath: Function[FtpFile, String], connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]]

  /**
   * Java API: creates a [[akka.stream.javadsl.Sink Sink]] of a [[FtpFile]] that removes a file.
   *
   * @param connectionSettings connection settings
   * @return A [[akka.stream.javadsl.Sink Sink]] of [[FtpFile]] that materializes to a [[java.util.concurrent.CompletionStage CompletionStage]] of [[IOResult]]
   */
  def remove(connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]]
}

object Ftp extends FtpApi[FTPClient, FtpSettings] with FtpSourceParams {
  def ls(host: String): Source[FtpFile, NotUsed] = ls(host, basePath = "")
  def ls(host: String, basePath: String): Source[FtpFile, NotUsed] = ls(basePath, defaultSettings(host))

  def ls(host: String, username: String, password: String): Source[FtpFile, NotUsed] =
    ls("", defaultSettings(host, Some(username), Some(password)))

  def ls(host: String, username: String, password: String, basePath: String): Source[FtpFile, NotUsed] =
    ls(basePath, defaultSettings(host, Some(username), Some(password)))

  def ls(basePath: String, connectionSettings: S): Source[FtpFile, NotUsed] =
    Source
      .fromGraph(createBrowserGraph(basePath, connectionSettings, f => true, _emitTraversedDirectories = false))

  def ls(basePath: String, connectionSettings: S, branchSelector: Predicate[FtpFile]): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(
        basePath,
        connectionSettings,
        asScalaFromPredicate(branchSelector),
        _emitTraversedDirectories = false
      )
    )

  def ls(basePath: String,
         connectionSettings: S,
         branchSelector: Predicate[FtpFile],
         emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(basePath, connectionSettings, asScalaFromPredicate(branchSelector), emitTraversedDirectories)
    )

  def fromPath(host: String, path: String): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, defaultSettings(host))

  def fromPath(host: String,
               username: String,
               password: String,
               path: String): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, defaultSettings(host, Some(username), Some(password)))

  def fromPath(path: String, connectionSettings: S): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, connectionSettings, DefaultChunkSize)

  def fromPath(path: String,
               connectionSettings: S,
               chunkSize: Int = DefaultChunkSize): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, connectionSettings, chunkSize, 0L)

  def fromPath(path: String,
               connectionSettings: S,
               chunkSize: Int,
               offset: Long): Source[ByteString, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Source
      .fromGraph(createIOSource(path, connectionSettings, chunkSize, offset))
      .mapMaterializedValue(_.toJava)
  }

  def toPath(path: String, connectionSettings: S, append: Boolean): Sink[ByteString, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Sink.fromGraph(createIOSink(path, connectionSettings, append)).mapMaterializedValue(_.toJava)
  }

  def toPath(path: String, connectionSettings: S): Sink[ByteString, CompletionStage[IOResult]] =
    toPath(path, connectionSettings, append = false)

  def move(destinationPath: Function[FtpFile, String],
           connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.compat.java8.FunctionConverters._
    import scala.compat.java8.FutureConverters._
    Sink
      .fromGraph(createMoveSink(destinationPath.asScala, connectionSettings))
      .mapMaterializedValue[CompletionStage[IOResult]](_.toJava)
  }

  def remove(connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Sink.fromGraph(createRemoveSink(connectionSettings)).mapMaterializedValue(_.toJava)
  }

}
object Ftps extends FtpApi[FTPSClient, FtpsSettings] with FtpsSourceParams {
  def ls(host: String): Source[FtpFile, NotUsed] = ls(host, basePath = "")
  def ls(host: String, basePath: String): Source[FtpFile, NotUsed] = ls(basePath, defaultSettings(host))

  def ls(host: String, username: String, password: String): Source[FtpFile, NotUsed] =
    ls("", defaultSettings(host, Some(username), Some(password)))

  def ls(host: String, username: String, password: String, basePath: String): Source[FtpFile, NotUsed] =
    ls(basePath, defaultSettings(host, Some(username), Some(password)))

  def ls(basePath: String, connectionSettings: S): Source[FtpFile, NotUsed] =
    Source
      .fromGraph(createBrowserGraph(basePath, connectionSettings, f => true, _emitTraversedDirectories = false))

  def ls(basePath: String, connectionSettings: S, branchSelector: Predicate[FtpFile]): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(
        basePath,
        connectionSettings,
        asScalaFromPredicate(branchSelector),
        _emitTraversedDirectories = false
      )
    )

  def ls(basePath: String,
         connectionSettings: S,
         branchSelector: Predicate[FtpFile],
         emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(basePath, connectionSettings, asScalaFromPredicate(branchSelector), emitTraversedDirectories)
    )

  def fromPath(host: String, path: String): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, defaultSettings(host))

  def fromPath(host: String,
               username: String,
               password: String,
               path: String): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, defaultSettings(host, Some(username), Some(password)))

  def fromPath(path: String, connectionSettings: S): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, connectionSettings, DefaultChunkSize)

  def fromPath(path: String,
               connectionSettings: S,
               chunkSize: Int = DefaultChunkSize): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, connectionSettings, chunkSize, 0L)

  def fromPath(path: String,
               connectionSettings: S,
               chunkSize: Int,
               offset: Long): Source[ByteString, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Source
      .fromGraph(createIOSource(path, connectionSettings, chunkSize, offset))
      .mapMaterializedValue(_.toJava)
  }

  def toPath(path: String, connectionSettings: S, append: Boolean): Sink[ByteString, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Sink.fromGraph(createIOSink(path, connectionSettings, append)).mapMaterializedValue(_.toJava)
  }

  def toPath(path: String, connectionSettings: S): Sink[ByteString, CompletionStage[IOResult]] =
    toPath(path, connectionSettings, append = false)

  def move(destinationPath: Function[FtpFile, String],
           connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.compat.java8.FunctionConverters._
    import scala.compat.java8.FutureConverters._
    Sink
      .fromGraph(createMoveSink(destinationPath.asScala, connectionSettings))
      .mapMaterializedValue[CompletionStage[IOResult]](_.toJava)
  }

  def remove(connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Sink.fromGraph(createRemoveSink(connectionSettings)).mapMaterializedValue(_.toJava)
  }

}

class SftpApi extends FtpApi[SSHClient, SftpSettings] with SftpSourceParams {
  def ls(host: String): Source[FtpFile, NotUsed] = ls(host, basePath = "")
  def ls(host: String, basePath: String): Source[FtpFile, NotUsed] = ls(basePath, defaultSettings(host))

  def ls(host: String, username: String, password: String): Source[FtpFile, NotUsed] =
    ls("", defaultSettings(host, Some(username), Some(password)))

  def ls(host: String, username: String, password: String, basePath: String): Source[FtpFile, NotUsed] =
    ls(basePath, defaultSettings(host, Some(username), Some(password)))

  def ls(basePath: String, connectionSettings: S): Source[FtpFile, NotUsed] =
    Source
      .fromGraph(createBrowserGraph(basePath, connectionSettings, f => true, _emitTraversedDirectories = false))

  def ls(basePath: String, connectionSettings: S, branchSelector: Predicate[FtpFile]): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(
        basePath,
        connectionSettings,
        asScalaFromPredicate(branchSelector),
        _emitTraversedDirectories = false
      )
    )

  def ls(basePath: String,
         connectionSettings: S,
         branchSelector: Predicate[FtpFile],
         emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(basePath, connectionSettings, asScalaFromPredicate(branchSelector), emitTraversedDirectories)
    )

  def fromPath(host: String, path: String): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, defaultSettings(host))

  def fromPath(host: String,
               username: String,
               password: String,
               path: String): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, defaultSettings(host, Some(username), Some(password)))

  def fromPath(path: String, connectionSettings: S): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, connectionSettings, DefaultChunkSize)

  def fromPath(path: String,
               connectionSettings: S,
               chunkSize: Int = DefaultChunkSize): Source[ByteString, CompletionStage[IOResult]] =
    fromPath(path, connectionSettings, chunkSize, 0L)

  def fromPath(path: String,
               connectionSettings: S,
               chunkSize: Int,
               offset: Long): Source[ByteString, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Source
      .fromGraph(createIOSource(path, connectionSettings, chunkSize, offset))
      .mapMaterializedValue(_.toJava)
  }

  def toPath(path: String, connectionSettings: S, append: Boolean): Sink[ByteString, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Sink.fromGraph(createIOSink(path, connectionSettings, append)).mapMaterializedValue(_.toJava)
  }

  def toPath(path: String, connectionSettings: S): Sink[ByteString, CompletionStage[IOResult]] =
    toPath(path, connectionSettings, append = false)

  def move(destinationPath: Function[FtpFile, String],
           connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.compat.java8.FunctionConverters._
    import scala.compat.java8.FutureConverters._
    Sink
      .fromGraph(createMoveSink(destinationPath.asScala, connectionSettings))
      .mapMaterializedValue[CompletionStage[IOResult]](_.toJava)
  }

  def remove(connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.compat.java8.FutureConverters._
    Sink.fromGraph(createRemoveSink(connectionSettings)).mapMaterializedValue(_.toJava)
  }

}
object Sftp extends SftpApi {

  /**
   * Java API: creates a [[akka.stream.alpakka.ftp.javadsl.SftpApi]]
   *
   * @param customSshClient custom ssh client
   * @return A [[akka.stream.alpakka.ftp.javadsl.SftpApi]]
   */
  def create(customSshClient: SSHClient): SftpApi =
    new SftpApi {
      override val sshClient = customSshClient
    }
}
