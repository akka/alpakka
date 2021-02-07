/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.util

import java.io.{File, InputStream, StringWriter}
import java.nio.ByteBuffer
import java.util

import akka.stream.alpakka.hdfs.RotationMessage
import akka.util.ByteString
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, Text}

import scala.collection.mutable.ListBuffer
import scala.language.higherKinds
import scala.util.Random
import org.scalatest.matchers.should.Matchers

sealed trait TestUtils {

  protected type Sequence[_]
  protected type Pair[_, _]
  protected type Assertion

  def read(stream: InputStream): String = {
    val writer = new StringWriter
    IOUtils.copy(stream, writer, "UTF-8")
    writer.toString
  }

  def getTestDir: File = {
    val targetDir = new File("hdfs/target")
    val testWorkingDir =
      new File(targetDir, s"hdfs-${System.currentTimeMillis}")
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs
    testWorkingDir
  }

  def setupCluster(): MiniDFSCluster = {
    val baseDir = new File(getTestDir, "miniHDFS")
    val conf = new HdfsConfiguration
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    val hdfsCluster = builder.nameNodePort(54310).format(true).build()
    hdfsCluster.waitClusterUp()
    hdfsCluster
  }

  def destination = "/tmp/alpakka/"

  def books: Sequence[ByteString]

  def booksForSequenceWriter: Sequence[Pair[Text, Text]]

  def getFiles(fs: FileSystem): Sequence[FileStatus]

  def generateFakeContent(count: Double, bytes: Long): Sequence[ByteString]

  def generateFakeContentWithPartitions(count: Double, bytes: Long, partition: Int): Sequence[ByteString]

  def readSequenceFile(fs: FileSystem, log: RotationMessage): Sequence[Pair[Text, Text]]

  def verifySequenceFile(fs: FileSystem,
                         content: Sequence[Pair[Text, Text]],
                         logs: Sequence[RotationMessage]): Assertion

  def generateFakeContentForSequence(count: Double, bytes: Long): Sequence[Pair[Text, Text]]

  def verifyOutputFileSize(fs: FileSystem, logs: Sequence[RotationMessage]): Assertion

  def readLogs(fs: FileSystem, logs: Sequence[RotationMessage]): Sequence[String]

  def readLogsWithFlatten(fs: FileSystem, logs: Sequence[RotationMessage]): Sequence[Char]

  def readLogsWithCodec(fs: FileSystem, logs: Sequence[RotationMessage], codec: CompressionCodec): Sequence[String]

  def verifyLogsWithCodec(fs: FileSystem,
                          content: Sequence[ByteString],
                          logs: Sequence[RotationMessage],
                          codec: CompressionCodec): Assertion
}

object ScalaTestUtils extends TestUtils with Matchers {
  type Sequence[A] = Seq[A]
  type Pair[A, B] = (A, B)
  type Assertion = org.scalatest.Assertion

  val books: Sequence[ByteString] = List(
    "Akka Concurrency",
    "Akka in Action",
    "Effective Akka",
    "Learning Scala",
    "Programming in Scala Programming"
  ).map(ByteString(_))

  val booksForSequenceWriter: Sequence[(Text, Text)] = books.zipWithIndex.map {
    case (data, index) =>
      new Text(index.toString) -> new Text(data.utf8String)
  }

  def getFiles(fs: FileSystem): Sequence[FileStatus] = {
    val p = new Path(destination)
    fs.listStatus(p)
  }

  def generateFakeContent(count: Double, bytes: Long): Sequence[ByteString] =
    ByteBuffer
      .allocate((count * bytes).toInt)
      .array()
      .toList
      .map(_ => Random.nextPrintableChar)
      .map(ByteString(_))

  def verifyOutputFileSize(fs: FileSystem, logs: Sequence[RotationMessage]): Assertion =
    ScalaTestUtils.getFiles(fs).size shouldEqual logs.size

  def verifyOutputFileSizeInArbitraryPath(f: () => Sequence[FileStatus], logs: Sequence[RotationMessage]): Assertion =
    f().size shouldEqual logs.size

  def readLogs(fs: FileSystem, logs: Sequence[RotationMessage]): Sequence[String] =
    logs.map(log => new Path(destination, log.path)).map(f => read(fs.open(f)))

  def readLogsWithFlatten(fs: FileSystem, logs: Sequence[RotationMessage]): Sequence[Char] =
    readLogs(fs, logs).flatten

  def generateFakeContentWithPartitions(count: Double, bytes: Long, partition: Int): Sequence[ByteString] = {
    val fakeData = generateFakeContent(count, bytes)
    val groupSize = Math.ceil(fakeData.size / partition.toDouble).toInt
    fakeData.grouped(groupSize).map(list => ByteString(list.map(_.utf8String).mkString)).toList
  }

  def readLogsWithCodec(fs: FileSystem, logs: Sequence[RotationMessage], codec: CompressionCodec): Sequence[String] =
    logs.map(log => new Path(destination, log.path)).map { file =>
      read(codec.createInputStream(fs.open(file)))
    }

  def verifyLogsWithCodec(fs: FileSystem,
                          content: Sequence[ByteString],
                          logs: Sequence[RotationMessage],
                          codec: CompressionCodec): Assertion = {
    val pureContent: String = content.map(_.utf8String).mkString
    val contentFromHdfsWithCodec: String = readLogsWithCodec(fs, logs, codec).mkString
    val contentFromHdfs: String = readLogs(fs, logs).mkString
    contentFromHdfs should !==(pureContent)
    contentFromHdfsWithCodec shouldEqual pureContent
  }

  def readSequenceFile(fs: FileSystem, log: RotationMessage): Sequence[(Text, Text)] = {
    val reader = new SequenceFile.Reader(fs.getConf, SequenceFile.Reader.file(new Path(destination, log.path)))
    var key = new Text
    var value = new Text
    val results = new ListBuffer[(Text, Text)]()
    while (reader.next(key, value)) {
      results += ((key, value))
      key = new Text
      value = new Text
    }
    results.toList
  }

  def verifySequenceFile(fs: FileSystem, content: Sequence[(Text, Text)], logs: Sequence[RotationMessage]): Assertion =
    logs.flatMap(readSequenceFile(fs, _)) shouldEqual content

  def generateFakeContentForSequence(count: Double, bytes: Long): Sequence[(Text, Text)] = {
    val half = ((count * bytes) / 2).toInt
    (0 to half)
      .map(_ => (new Text(Random.nextPrintableChar.toString), new Text(Random.nextPrintableChar.toString)))
      .toList
  }
}

object JavaTestUtils extends TestUtils {
  type Sequence[A] = java.util.List[A]
  type Pair[A, B] = akka.japi.Pair[A, B]
  type Assertion = Unit

  import org.junit.Assert._

  import scala.collection.JavaConverters._

  val books: util.List[ByteString] = ScalaTestUtils.books.asJava

  val booksForSequenceWriter: util.List[Pair[Text, Text]] =
    ScalaTestUtils.booksForSequenceWriter.map { case (k, v) => akka.japi.Pair(k, v) }.asJava

  def getFiles(fs: FileSystem): Sequence[FileStatus] =
    ScalaTestUtils.getFiles(fs).asJava

  def generateFakeContent(count: Double, bytes: Long): Sequence[ByteString] =
    ScalaTestUtils.generateFakeContent(count, bytes).asJava

  def verifyOutputFileSize(fs: FileSystem, logs: Sequence[RotationMessage]): Unit =
    assertEquals(getFiles(fs).size(), logs.size())

  def readLogs(fs: FileSystem, logs: Sequence[RotationMessage]): Sequence[String] =
    ScalaTestUtils.readLogs(fs, logs.asScala.toIndexedSeq).asJava

  def readLogsWithFlatten(fs: FileSystem, logs: Sequence[RotationMessage]): Sequence[Char] =
    ScalaTestUtils.readLogsWithFlatten(fs, logs.asScala.toIndexedSeq).asJava

  def generateFakeContentWithPartitions(count: Double, bytes: Long, partition: Int): Sequence[ByteString] =
    ScalaTestUtils.generateFakeContentWithPartitions(count, bytes, partition).asJava

  def verifyLogsWithCodec(fs: FileSystem,
                          content: Sequence[ByteString],
                          logs: Sequence[RotationMessage],
                          codec: CompressionCodec): Assertion = {
    val pureContent: String = content.asScala.map(_.utf8String).mkString
    val contentFromHdfsWithCodec: String =
      ScalaTestUtils.readLogsWithCodec(fs, logs.asScala.toIndexedSeq, codec).mkString
    val contentFromHdfs: String = ScalaTestUtils.readLogs(fs, logs.asScala.toIndexedSeq).mkString
    assertNotEquals(contentFromHdfs, pureContent)
    assertEquals(contentFromHdfsWithCodec, pureContent)
  }

  def readSequenceFile(fs: FileSystem, log: RotationMessage): Sequence[Pair[Text, Text]] =
    ScalaTestUtils.readSequenceFile(fs, log).map { case (k, v) => akka.japi.Pair(k, v) }.asJava

  def verifySequenceFile(fs: FileSystem, content: Sequence[Pair[Text, Text]], logs: Sequence[RotationMessage]): Unit =
    assertArrayEquals(logs.asScala.flatMap(readSequenceFile(fs, _).asScala).asJava.toArray, content.toArray)

  def generateFakeContentForSequence(count: Double, bytes: Long): Sequence[Pair[Text, Text]] =
    ScalaTestUtils.generateFakeContentForSequence(count, bytes).map { case (k, v) => akka.japi.Pair(k, v) }.asJava

  def readLogsWithCodec(fs: FileSystem, logs: Sequence[RotationMessage], codec: CompressionCodec): Sequence[String] =
    ScalaTestUtils.readLogsWithCodec(fs, logs.asScala.toIndexedSeq, codec).asJava

  def verifyFlattenContent(fs: FileSystem, logs: Sequence[RotationMessage], content: Sequence[ByteString]): Unit =
    assertArrayEquals(readLogsWithFlatten(fs, logs).toArray, content.asScala.flatMap(_.utf8String).asJava.toArray)

}
