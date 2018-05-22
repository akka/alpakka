/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs

import java.util.function.BiFunction

import org.apache.hadoop.fs.Path

private[hdfs] sealed trait FilePathGenerator extends FilePathGenerator.F {
  def tempDirectory: String
}

object FilePathGenerator {
  private type F = (Long, Long) => Path
  private val DefaultTempDirectory = "/tmp/alpakka-hdfs"

  /*
   * Scala API: creates [[FilePathGenerator]] to rotate output
   * @param f a function that takes rotation count and timestamp to return path of output
   * @param temp the temporary directory that [[HdfsFlowStage]] use
   */
  def create(f: (Long, Long) => String, temp: String = DefaultTempDirectory): FilePathGenerator =
    new FilePathGenerator {
      val tempDirectory: String = temp
      def apply(rotationCount: Long, timestamp: Long): Path = new Path(f(rotationCount, timestamp))
    }

  /*
   * Java API: creates [[FilePathGenerator]] to rotate output
   * @param f a function that takes rotation count and timestamp to return path of output
   */
  def create(f: BiFunction[Long, Long, String]): FilePathGenerator =
    create(f.apply _, DefaultTempDirectory)

  /*
   * Java API: creates [[FilePathGenerator]] to rotate output
   * @param f a function that takes rotation count and timestamp to return path of output
   */
  def create(f: BiFunction[Long, Long, String], temp: String): FilePathGenerator =
    create(f.apply _, temp)

}
