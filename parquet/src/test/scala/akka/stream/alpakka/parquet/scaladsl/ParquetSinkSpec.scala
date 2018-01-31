/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.parquet.scaladsl

import java.net.URI
import java.nio.file
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.stream.Collectors

import akka.actor.ActorSystem
import akka.stream.Supervision.{Decider, Stop}
import akka.stream.alpakka.parquet.Parquet.{ParquetSettings, PartitionSettings}
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationLong

class ParquetSinkSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  override implicit val patienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(500, Milliseconds))

  val decider: Decider = {
    case t: Throwable =>
      t.printStackTrace()
      Stop
  }

  implicit val system = ActorSystem("ParquetSpec")
  implicit val mat = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "The Parquet Sink" should {
    "write elements to plain sink" in {

      val tmpDir = Files.createTempDirectory("parquet-spec-").toFile
      tmpDir.deleteOnExit()
      val baseDir = s"${tmpDir.toURI}/basedir"
      val settings = ParquetSettings(baseDir)
        .withWriteMode(Mode.CREATE)
      val fileName = UUID.randomUUID().toString
      val plainSink = ParquetSink[DataRecord](settings, fileName)

      val completion = Source(0 until 10)
        .map(i => new DataRecord(i.toString, s"TestData-$i"))
        .runWith(plainSink)

      whenReady(completion) { done =>
        val records = readParquetFile[DataRecord](s"$baseDir/$fileName.parquet").toList

        records.size shouldBe 10

        (0 until 10).foreach(i => {
          val record = records(i)
          record.id shouldBe i.toString
          record.data shouldBe s"TestData-$i"
        })
      }

    }

    "write elements to partitioned sink" in {
      val tmpDir = Files.createTempDirectory("parquet-spec-").toFile
      tmpDir.deleteOnExit()
      val baseDir = s"${tmpDir.toURI}/basedir"
      val settings = ParquetSettings(baseDir)
        .withWriteMode(Mode.OVERWRITE)
        .withPartitionSettings(PartitionSettings(10, 10 seconds, 4))
      val partitionSink =
        ParquetSink.partitionSink[DataRecord, Int](settings, "parquestPartition")(r => r.id.toInt % 10,
                                                                                  i => i.toString)

      val completion = Source(0 until 100)
        .map(i => new DataRecord(i.toString, s"PartitionTestData-$i"))
        .runWith(partitionSink)

      whenReady(completion) { done =>
        val files = traverseDirectory(baseDir)
        val records = files.flatMap(f => readParquetFile[DataRecord](f.toUri.toString)).sortBy(_.id.toInt)

        records.size shouldBe 100

        (0 until 100).foreach(i => {
          val record = records(i)
          record.id shouldBe i.toString
          record.data shouldBe s"PartitionTestData-$i"
        })

      }

    }
  }

  private def readParquetFile[T](path: String) = {
    val conf = new Configuration()
    conf.setInt(ParquetFileReader.PARQUET_READ_PARALLELISM, 5)
    val reader = AvroParquetReader
      .builder[T](new Path(path))
      .withDataModel(ReflectData.get())
      .withConf(conf)
      .build()
    Iterator.continually(reader.read()).takeWhile(_ != null)
  }

  private def traverseDirectory(baseDir: String): List[file.Path] =
    Files
      .list(Paths.get(URI.create(baseDir)))
      .collect(Collectors.toList[file.Path])
      .asScala
      .toList
      .flatMap(p => {
        if (Files.isDirectory(p)) {
          traverseDirectory(p.toUri.toString)
        } else {
          List[file.Path](p)
        }
      })
}

class DataRecord(val id: String, val data: String) {
  def this() = this(null, null)
}
