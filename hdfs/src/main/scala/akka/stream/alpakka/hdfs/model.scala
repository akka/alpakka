/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs

import java.util.function.BiFunction

import akka.NotUsed
import akka.stream.alpakka.hdfs.impl.HdfsFlowLogic
import akka.stream.alpakka.hdfs.impl.strategy.DefaultRotationStrategy._
import akka.stream.alpakka.hdfs.impl.strategy.DefaultSyncStrategy._
import akka.stream.alpakka.hdfs.impl.strategy.Strategy
import akka.util.ByteString
import org.apache.hadoop.fs.Path

import scala.concurrent.duration.FiniteDuration

final class HdfsWritingSettings private (
    val overwrite: Boolean,
    val newLine: Boolean,
    val lineSeparator: String,
    val pathGenerator: FilePathGenerator
) {
  private[hdfs] val newLineByteArray = ByteString(lineSeparator).toArray

  def withOverwrite(value: Boolean): HdfsWritingSettings = if (overwrite == value) this else copy(overwrite = value)
  def withNewLine(value: Boolean): HdfsWritingSettings = if (newLine == value) this else copy(newLine = value)
  def withLineSeparator(value: String): HdfsWritingSettings = copy(lineSeparator = value)
  def withPathGenerator(value: FilePathGenerator): HdfsWritingSettings = copy(pathGenerator = value)

  private def copy(
      overwrite: Boolean = overwrite,
      newLine: Boolean = newLine,
      lineSeparator: String = lineSeparator,
      pathGenerator: FilePathGenerator = pathGenerator
  ): HdfsWritingSettings = new HdfsWritingSettings(
    overwrite = overwrite,
    newLine = newLine,
    lineSeparator = lineSeparator,
    pathGenerator = pathGenerator
  )

  override def toString =
    "HdfsWritingSettings(" +
    s"overwrite=$overwrite," +
    s"newLine=$newLine," +
    s"lineSeparator=$lineSeparator," +
    s"pathGenerator=$pathGenerator" +
    ")"
}

object HdfsWritingSettings {

  private val DefaultFilePathGenerator: FilePathGenerator =
    FilePathGenerator((rc: Long, _: Long) => s"/tmp/alpakka/$rc")

  val default = new HdfsWritingSettings(
    overwrite = true,
    newLine = false,
    lineSeparator = System.getProperty("line.separator"),
    pathGenerator = DefaultFilePathGenerator
  )

  /** Scala API */
  def apply(): HdfsWritingSettings = default

  /** Java API */
  def create(): HdfsWritingSettings = default
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

/**
 * Class `RotationMessage` represents an outgoing message of the rotation event
 *
 * @param path     an absolute path of an output file in Hdfs
 * @param rotation a number of rotation of an output
 */
final case class RotationMessage(path: String, rotation: Int) extends OutgoingMessage[Nothing]

/**
 * Class `WrittenMessage` represents an outgoing message of the writing event
 *
 * @param passThrough a value of pass-through
 * @param inRotation  a number of the rotation that writing event occurred
 * @tparam P type of the value of pass-through
 */
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
  protected[hdfs] def preStart[W, I, C](logic: HdfsFlowLogic[W, I, C]): Unit
}

object RotationStrategy {

  /**
   * Creates a rotation strategy that will trigger a file rotation
   * after a certain size of messages have been processed.
   *
   * @param count a count of [[FileUnit]]
   * @param unit a file unit
   */
  def size(count: Double, unit: FileUnit): RotationStrategy =
    SizeRotationStrategy(0, count * unit.byteCount)

  /**
   * Creates a rotation strategy that will trigger a file rotation
   * after a certain number of messages have been processed.
   *
   * @param count message count to rotate files
   */
  def count(count: Long): RotationStrategy =
    CountRotationStrategy(0, count)

  /**
   * Creates a rotation strategy that will trigger a file rotation
   * after a finite duration.
   *
   * @param interval duration to rotate files
   */
  def time(interval: FiniteDuration): RotationStrategy =
    TimeRotationStrategy(interval)

  /**
   * Creates a non-functioning rotation strategy that will not trigger
   * a file rotation, mostly suitable for finite streams and testing.
   */
  def none: RotationStrategy =
    NoRotationStrategy
}

abstract class SyncStrategy extends Strategy {
  type S = SyncStrategy
}

object SyncStrategy {

  /**
   * Creates a synchronization strategy that will trigger the Hadoop file system
   * sync after a certain number of messages have been processed.
   *
   * @param count message count to synchronize the output
   */
  def count(count: Long): SyncStrategy = CountSyncStrategy(0, count)

  /**
   * Creates a non-functioning synchronization strategy that will not trigger
   * the Hadoop file system sync.
   */
  def none: SyncStrategy = NoSyncStrategy

}
