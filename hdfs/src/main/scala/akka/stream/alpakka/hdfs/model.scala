/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs

import java.util.function.BiFunction

import akka.NotUsed
import akka.stream.alpakka.hdfs.HdfsWritingSettings._
import akka.stream.alpakka.hdfs.impl.strategy.DefaultRotationStrategy._
import akka.stream.alpakka.hdfs.impl.strategy.DefaultSyncStrategy._
import akka.stream.alpakka.hdfs.impl.strategy.Strategy
import org.apache.hadoop.fs.Path

import scala.concurrent.duration.FiniteDuration

final case class HdfsWritingSettings(
    overwrite: Boolean = true,
    newLine: Boolean = false,
    pathGenerator: FilePathGenerator = DefaultFilePathGenerator
) {
  def withOverwrite(overwrite: Boolean): HdfsWritingSettings = copy(overwrite = overwrite)
  def withNewLine(newLine: Boolean): HdfsWritingSettings = copy(newLine = newLine)
  def withPathGenerator(generator: FilePathGenerator): HdfsWritingSettings = copy(pathGenerator = generator)
}

object HdfsWritingSettings {

  private val DefaultFilePathGenerator: FilePathGenerator =
    FilePathGenerator((rc: Long, _: Long) => s"/tmp/alpakka/$rc")

  /**
   * Java API
   */
  def create(): HdfsWritingSettings =
    HdfsWritingSettings()
}

final case class HdfsWriteMessage[T, P](source: T, passThrough: P)

object HdfsWriteMessage {

  /**
   * Scala API - creates [[HdfsWriteMessage]] to use when not using passThrough
   *
   * @param source a message
   */
  def apply[T](source: T): HdfsWriteMessage[T, NotUsed] =
    HdfsWriteMessage(source, NotUsed)

  /**
   * Java API - creates [[HdfsWriteMessage]] to use when not using passThrough
   *
   * @param source a message
   */
  def create[T](source: T): HdfsWriteMessage[T, NotUsed] =
    HdfsWriteMessage(source)

  /**
   * Java API - creates [[HdfsWriteMessage]] to use with passThrough
   *
   * @param source a message
   * @param passThrough pass-through data
   */
  def create[T, P](source: T, passThrough: P): HdfsWriteMessage[T, P] =
    HdfsWriteMessage(source, passThrough)

}

sealed abstract class OutgoingMessage[+P]
final case class RotationMessage(path: String, rotation: Int) extends OutgoingMessage[Nothing]
final case class WrittenMessage[P](passThrough: P, inRotation: Int) extends OutgoingMessage[P]

sealed case class FileUnit(byteCount: Long)

object FileUnit {
  val KB = FileUnit(Math.pow(2, 10).toLong)
  val MB = FileUnit(Math.pow(2, 20).toLong)
  val GB = FileUnit(Math.pow(2, 30).toLong)
  val TB = FileUnit(Math.pow(2, 40).toLong)
}

sealed abstract class FilePathGenerator extends ((Long, Long) => Path) {
  def tempDirectory: String
}

object FilePathGenerator {
  private val DefaultTempDirectory = "/tmp/alpakka-hdfs"

  /**
   * Scala API: creates [[FilePathGenerator]] to rotate output
   *
   * @param f    a function that takes rotation count and timestamp to return path of output
   * @param temp the temporary directory that [[akka.stream.alpakka.hdfs.impl.HdfsFlowStage]] use
   */
  def apply(f: (Long, Long) => String, temp: String = DefaultTempDirectory): FilePathGenerator =
    new FilePathGenerator {
      val tempDirectory: String = temp
      def apply(rotationCount: Long, timestamp: Long): Path = new Path(f(rotationCount, timestamp))
    }

  /**
   * Java API: creates [[FilePathGenerator]] to rotate output
   *
   * @param f a function that takes rotation count and timestamp to return path of output
   */
  def create(f: BiFunction[java.lang.Long, java.lang.Long, String]): FilePathGenerator =
    FilePathGenerator(javaFuncToScalaFunc(f), DefaultTempDirectory)

  /**
   * Java API: creates [[FilePathGenerator]] to rotate output
   *
   * @param f a function that takes rotation count and timestamp to return path of output
   */
  def create(f: BiFunction[java.lang.Long, java.lang.Long, String], temp: String): FilePathGenerator =
    FilePathGenerator(javaFuncToScalaFunc(f), temp)

  private def javaFuncToScalaFunc(f: BiFunction[java.lang.Long, java.lang.Long, String]): (Long, Long) => String =
    (rc, t) => f.apply(rc, t)

}

abstract class RotationStrategy extends Strategy {
  type S = RotationStrategy
}

object RotationStrategy {

  /**
   * Creates [[SizeRotationStrategy]]
   *
   * @param count a count of [[FileUnit]]
   * @param unit a file unit
   */
  def size(count: Double, unit: FileUnit): RotationStrategy =
    SizeRotationStrategy(0, count * unit.byteCount)

  /**
   * Creates [[CountRotationStrategy]]
   *
   * @param count message count to rotate files
   */
  def count(count: Long): RotationStrategy =
    CountRotationStrategy(0, count)

  /**
   * Creates [[TimeRotationStrategy]]
   *
   * @param interval duration to rotate files
   */
  def time(interval: FiniteDuration): RotationStrategy =
    TimeRotationStrategy(interval)

  /**
   * Creates [[NoRotationStrategy]]
   */
  def none: RotationStrategy =
    NoRotationStrategy
}

abstract class SyncStrategy extends Strategy {
  type S = SyncStrategy
}

object SyncStrategy {

  /**
   * Creates [[CountSyncStrategy]]
   *
   * @param count message count to synchronize the output
   */
  def count(count: Long): SyncStrategy = CountSyncStrategy(0, count)

  /**
   * Creates [[NoSyncStrategy]]
   */
  def none: SyncStrategy = NoSyncStrategy

}
