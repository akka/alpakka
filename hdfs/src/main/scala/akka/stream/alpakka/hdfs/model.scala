/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs

import java.util.function.BiFunction

import akka.NotUsed
import akka.stream.alpakka.hdfs.HdfsWritingSettings._
import akka.stream.alpakka.hdfs.impl.strategy.{RotationStrategy, SyncStrategy}
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

final case class IncomingMessage[T, C](source: T, passThrough: C)

object IncomingMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source, NotUsed)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source)

  // Java-api - with passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(source, passThrough)

}

sealed abstract class OutgoingMessage[+T]
final case class RotationMessage(path: String, rotation: Int) extends OutgoingMessage[Nothing]
final case class WrittenMessage[T](passThrough: T, inRotation: Int) extends OutgoingMessage[T]

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

  /*
   * Scala API: creates [[FilePathGenerator]] to rotate output
   * @param f a function that takes rotation count and timestamp to return path of output
   * @param temp the temporary directory that [[HdfsFlowStage]] use
   */
  def apply(f: (Long, Long) => String, temp: String = DefaultTempDirectory): FilePathGenerator =
    new FilePathGenerator {
      val tempDirectory: String = temp
      def apply(rotationCount: Long, timestamp: Long): Path = new Path(f(rotationCount, timestamp))
    }

  /*
   * Java API: creates [[FilePathGenerator]] to rotate output
   * @param f a function that takes rotation count and timestamp to return path of output
   */
  def create(f: BiFunction[java.lang.Long, java.lang.Long, String]): FilePathGenerator =
    FilePathGenerator(javaFuncToScalaFunc(f), DefaultTempDirectory)

  /*
   * Java API: creates [[FilePathGenerator]] to rotate output
   * @param f a function that takes rotation count and timestamp to return path of output
   */
  def create(f: BiFunction[java.lang.Long, java.lang.Long, String], temp: String): FilePathGenerator =
    FilePathGenerator(javaFuncToScalaFunc(f), temp)

  private def javaFuncToScalaFunc(f: BiFunction[java.lang.Long, java.lang.Long, String]): (Long, Long) => String =
    (rc, t) => f.apply(rc, t)

}

object RotationStrategyFactory {

  import impl.strategy.RotationStrategy._

  /*
   * Creates [[SizeRotationStrategy]]
   * @param count a count of [[FileUnit]]
   * @param unit [[FileUnit]]
   */
  def size(count: Double, unit: FileUnit): RotationStrategy =
    SizeRotationStrategy(0, count * unit.byteCount)

  /*
   * Creates [[CountedRotationStrategy]]
   * @param count message count to rotate files
   */
  def count(count: Long): RotationStrategy =
    CountRotationStrategy(0, count)

  /*
   * Creates [[TimedRotationStrategy]]
   * @param interval duration to rotate files
   */
  def time(interval: FiniteDuration): RotationStrategy =
    TimeRotationStrategy(interval)

  /*
   * Creates [[NoRotationStrategy]]
   */
  def none: RotationStrategy =
    NoRotationStrategy
}

object SyncStrategyFactory {

  import impl.strategy.SyncStrategy._

  /*
   * Creates [[CountSyncStrategy]]
   * @param count message count to synchronize the output
   */
  def count(count: Long): SyncStrategy = CountSyncStrategy(0, count)

  /*
   * Creates [[NoSyncStrategy]]
   */
  def none: SyncStrategy = NoSyncStrategy

}
