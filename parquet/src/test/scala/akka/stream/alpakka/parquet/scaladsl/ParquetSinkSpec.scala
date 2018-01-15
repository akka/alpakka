/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.parquet.scaladsl

import java.nio.file.Files
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Supervision.{Decider, Stop}
import akka.stream.alpakka.parquet.Parquet.ParquetSettings
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.junit.runner.RunWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration.DurationLong

@RunWith(classOf[JUnitRunner])
class ParquetSinkSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  override implicit val patienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(500, Milliseconds))

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
      val settings = ParquetSettings(baseDir, 10, 10 seconds)
        .withWriteMode(Mode.CREATE)
      val fileName = UUID.randomUUID().toString
      val plainSink = ParquetSink[TestRecord](settings, fileName)

      val completion = Source(0 until 10)
        .map(i => new TestRecord(i.toString, s"TestData-$i"))
        .runWith(plainSink)

      whenReady(completion) { done =>
        val records = readParquetFile[TestRecord](s"$baseDir/$fileName.parquet").toList

        records.size shouldBe 10

        (0 until 10).foreach(i => {
          val record = records(i)
          record.id shouldBe i.toString
          record.data shouldBe s"TestData-$i"
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
}

class TestRecord(val id: String, val data: String) {
  def this() = this(null, null)
}
