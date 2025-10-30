/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.ftp.javadsl

import akka.actor.ClassicActorSystemProvider
import java.util.concurrent.CompletionStage
import java.util.function._

import akka.annotation.DoNotInherit
import akka.stream.alpakka.ftp._
import akka.stream.alpakka.ftp.impl._
import akka.stream.javadsl.{Sink, Source}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import akka.{Done, NotUsed}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

import scala.jdk.FunctionConverters._

@DoNotInherit
sealed trait FtpApi[FtpClient, S <: RemoteFileSettings] { factory: FtpSourceFactory[FtpClient, S] =>

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
  def mkdir(basePath: String, name: String, connectionSettings: S): Source[Done, NotUsed]

  /**
   * Java API for creating a directory in a given path
   *
   * @param basePath path to start with
   * @param name name of a directory to create
   * @param connectionSettings connection settings
   * @param materializer materializer
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of [[akka.Done]] indicating a materialized, asynchronous request
   * @deprecated pass in the actor system instead of the materializer, since 3.0.0
   */
  @Deprecated
  def mkdirAsync(basePath: String, name: String, connectionSettings: S, mat: Materializer): CompletionStage[Done]

  /**
   * Java API for creating a directory in a given path
   *
   * @param basePath path to start with
   * @param name name of a directory to create
   * @param connectionSettings connection settings
   * @param system actor system
   * @return [[java.util.concurrent.CompletionStage CompletionStage]] of [[akka.Done]] indicating a materialized, asynchronous request
   */
  def mkdirAsync(basePath: String,
                 name: String,
                 connectionSettings: S,
                 system: ClassicActorSystemProvider): CompletionStage[Done]

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

  protected[javadsl] def func[T, R](f: T => R): akka.japi.function.Function[T, R] =
    new akka.japi.function.Function[T, R] {
      override def apply(param: T): R = f(param)
    }
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
      .fromGraph(createBrowserGraph(basePath, connectionSettings, _ => true, _emitTraversedDirectories = false))

  def ls(basePath: String, connectionSettings: S, branchSelector: Predicate[FtpFile]): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(
        basePath,
        connectionSettings,
        branchSelector.asScala,
        _emitTraversedDirectories = false
      )
    )

  def ls(basePath: String,
         connectionSettings: S,
         branchSelector: Predicate[FtpFile],
         emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(basePath, connectionSettings, branchSelector.asScala, emitTraversedDirectories)
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
    import scala.jdk.FutureConverters._
    Source
      .fromGraph(createIOSource(path, connectionSettings, chunkSize, offset))
      .mapMaterializedValue(func(_.asJava))
  }

  def mkdir(basePath: String, name: String, connectionSettings: S): Source[Done, NotUsed] =
    Source.fromGraph(createMkdirGraph(basePath, name, connectionSettings)).map(func(_ => Done))

  def mkdirAsync(basePath: String, name: String, connectionSettings: S, mat: Materializer): CompletionStage[Done] = {
    val sink: Sink[Done, CompletionStage[Done]] = Sink.head()
    mkdir(basePath, name, connectionSettings).runWith(sink, mat)
  }

  def mkdirAsync(basePath: String,
                 name: String,
                 connectionSettings: S,
                 system: ClassicActorSystemProvider): CompletionStage[Done] = {
    mkdirAsync(basePath, name, connectionSettings, system.classicSystem)
  }

  def toPath(path: String, connectionSettings: S, append: Boolean): Sink[ByteString, CompletionStage[IOResult]] = {
    import scala.jdk.FutureConverters._
    Sink.fromGraph(createIOSink(path, connectionSettings, append)).mapMaterializedValue(func(_.asJava))
  }

  def toPath(path: String, connectionSettings: S): Sink[ByteString, CompletionStage[IOResult]] =
    toPath(path, connectionSettings, append = false)

  def move(destinationPath: Function[FtpFile, String],
           connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.jdk.FunctionConverters._
    import scala.jdk.FutureConverters._
    Sink
      .fromGraph(createMoveSink(destinationPath.asScala, connectionSettings))
      .mapMaterializedValue[CompletionStage[IOResult]](func(_.asJava))
  }

  def remove(connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.jdk.FutureConverters._
    Sink.fromGraph(createRemoveSink(connectionSettings)).mapMaterializedValue(func(_.asJava))
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
      .fromGraph(createBrowserGraph(basePath, connectionSettings, _ => true, _emitTraversedDirectories = false))

  def ls(basePath: String, connectionSettings: S, branchSelector: Predicate[FtpFile]): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(
        basePath,
        connectionSettings,
        branchSelector.asScala,
        _emitTraversedDirectories = false
      )
    )

  def ls(basePath: String,
         connectionSettings: S,
         branchSelector: Predicate[FtpFile],
         emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(basePath, connectionSettings, branchSelector.asScala, emitTraversedDirectories)
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
    import scala.jdk.FutureConverters._
    Source
      .fromGraph(createIOSource(path, connectionSettings, chunkSize, offset))
      .mapMaterializedValue(func(_.asJava))
  }

  def mkdir(basePath: String, name: String, connectionSettings: S): Source[Done, NotUsed] =
    Source.fromGraph(createMkdirGraph(basePath, name, connectionSettings)).map(func(_ => Done))

  def mkdirAsync(basePath: String, name: String, connectionSettings: S, mat: Materializer): CompletionStage[Done] = {
    val sink: Sink[Done, CompletionStage[Done]] = Sink.head()
    mkdir(basePath, name, connectionSettings).runWith(sink, mat)
  }

  def mkdirAsync(basePath: String,
                 name: String,
                 connectionSettings: S,
                 system: ClassicActorSystemProvider): CompletionStage[Done] = {
    mkdirAsync(basePath, name, connectionSettings, system.classicSystem)
  }

  def toPath(path: String, connectionSettings: S, append: Boolean): Sink[ByteString, CompletionStage[IOResult]] = {
    import scala.jdk.FutureConverters._
    Sink.fromGraph(createIOSink(path, connectionSettings, append)).mapMaterializedValue(func(_.asJava))
  }

  def toPath(path: String, connectionSettings: S): Sink[ByteString, CompletionStage[IOResult]] =
    toPath(path, connectionSettings, append = false)

  def move(destinationPath: Function[FtpFile, String],
           connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.jdk.FunctionConverters._
    import scala.jdk.FutureConverters._
    Sink
      .fromGraph(createMoveSink(destinationPath.asScala, connectionSettings))
      .mapMaterializedValue(func(_.asJava))
  }

  def remove(connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.jdk.FutureConverters._
    Sink.fromGraph(createRemoveSink(connectionSettings)).mapMaterializedValue(func(_.asJava))
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
      .fromGraph(createBrowserGraph(basePath, connectionSettings, _ => true, _emitTraversedDirectories = false))

  def ls(basePath: String, connectionSettings: S, branchSelector: Predicate[FtpFile]): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(
        basePath,
        connectionSettings,
        branchSelector.asScala,
        _emitTraversedDirectories = false
      )
    )

  def ls(basePath: String,
         connectionSettings: S,
         branchSelector: Predicate[FtpFile],
         emitTraversedDirectories: Boolean): Source[FtpFile, NotUsed] =
    Source.fromGraph(
      createBrowserGraph(basePath, connectionSettings, branchSelector.asScala, emitTraversedDirectories)
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
    import scala.jdk.FutureConverters._
    Source
      .fromGraph(createIOSource(path, connectionSettings, chunkSize, offset))
      .mapMaterializedValue(func(_.asJava))
  }

  def mkdir(basePath: String, name: String, connectionSettings: S): Source[Done, NotUsed] =
    Source.fromGraph(createMkdirGraph(basePath, name, connectionSettings)).map(func(_ => Done))

  def mkdirAsync(basePath: String, name: String, connectionSettings: S, mat: Materializer): CompletionStage[Done] = {
    val sink: Sink[Done, CompletionStage[Done]] = Sink.head()
    mkdir(basePath, name, connectionSettings).runWith(sink, mat)
  }

  def mkdirAsync(basePath: String,
                 name: String,
                 connectionSettings: S,
                 system: ClassicActorSystemProvider): CompletionStage[Done] = {
    mkdirAsync(basePath, name, connectionSettings, system.classicSystem)
  }

  def toPath(path: String, connectionSettings: S, append: Boolean): Sink[ByteString, CompletionStage[IOResult]] = {
    import scala.jdk.FutureConverters._
    Sink.fromGraph(createIOSink(path, connectionSettings, append)).mapMaterializedValue(func(_.asJava))
  }

  def toPath(path: String, connectionSettings: S): Sink[ByteString, CompletionStage[IOResult]] =
    toPath(path, connectionSettings, append = false)

  def move(destinationPath: Function[FtpFile, String],
           connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.jdk.FunctionConverters._
    import scala.jdk.FutureConverters._
    Sink
      .fromGraph(createMoveSink(destinationPath.asScala, connectionSettings))
      .mapMaterializedValue(func(_.asJava))
  }

  def remove(connectionSettings: S): Sink[FtpFile, CompletionStage[IOResult]] = {
    import scala.jdk.FutureConverters._
    Sink.fromGraph(createRemoveSink(connectionSettings)).mapMaterializedValue(func(_.asJava))
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
      override def sshClient(): SSHClient = customSshClient
    }
}
