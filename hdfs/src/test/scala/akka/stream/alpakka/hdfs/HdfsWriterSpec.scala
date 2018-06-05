/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.hdfs.scaladsl.HdfsFlow
import akka.stream.alpakka.hdfs.util.ScalaTestUtils._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.scalatest._

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContextExecutor}

class HdfsWriterSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var hdfsCluster: MiniDFSCluster = _
  private val destionation = "/tmp/alpakka/"

  //#init-mat
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  //#init-mat
  //#init-client
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.FileSystem

  val conf = new Configuration()
  conf.set("fs.default.name", "hdfs://localhost:54310")

  var fs: FileSystem = FileSystem.get(conf)
  //#init-client

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  fs.getConf.setEnum("zlib.compress.level", CompressionLevel.BEST_SPEED)

  val settings = HdfsWritingSettings()

  "DataWriter" should {
    "use file size rotation and produce five files" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategyFactory.count(50),
        RotationStrategyFactory.size(0.01, FileUnit.KB),
        settings
      )

      val resF = Source
        .fromIterator(() => books.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldBe Seq(
        RotationMessage("0", 0),
        RotationMessage("1", 1),
        RotationMessage("2", 2),
        RotationMessage("3", 3),
        RotationMessage("4", 4)
      )

      verifyOutputFileSize(fs, logs)
      readLogs(fs, logs) shouldBe books.map(_.utf8String)
    }

    "use file size rotation and produce exactly two files" in {
      val data = generateFakeContent(1, FileUnit.KB.byteCount)
      val dataIterator = data.toIterator

      val flow = HdfsFlow.data(
        fs,
        SyncStrategyFactory.count(500),
        RotationStrategyFactory.size(0.5, FileUnit.KB),
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => dataIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs.size shouldEqual 2

      val files = getFiles(fs)
      files.map(_.getLen).sum shouldEqual 1024
      files.foreach(f => f.getLen should be <= 512L)

      verifyOutputFileSize(fs, logs)
      readLogsWithFlatten(fs, logs) shouldBe data.flatMap(_.utf8String)
    }

    "detect upstream finish and move remaining data to hdfs" in {
      // Use huge rotation
      val data = generateFakeContent(1, FileUnit.KB.byteCount)
      val dataIterator = data.toIterator
      //#define-data
      val flow = HdfsFlow.data(
        fs,
        SyncStrategyFactory.count(500),
        RotationStrategyFactory.size(1, FileUnit.GB),
        HdfsWritingSettings()
      )
      //#define-data
      val resF = Source
        .fromIterator(() => dataIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs.size shouldEqual 1

      getFiles(fs).head.getLen shouldEqual 1024

      verifyOutputFileSize(fs, logs)
      readLogsWithFlatten(fs, logs) shouldBe data.flatMap(_.utf8String)
    }

    "use buffer rotation and produce three files" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategyFactory.count(1),
        RotationStrategyFactory.count(2),
        settings
      )

      val resF = Source
        .fromIterator(() => books.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs.size shouldEqual 3

      verifyOutputFileSize(fs, logs)
      readLogs(fs, logs) shouldBe books.map(_.utf8String).grouped(2).map(_.mkString).toSeq
    }

    "use time rotation" in {
      val (cancellable, resF) = Source
        .tick(0.millis, 50.milliseconds, ByteString("I love Alpakka!"))
        .map(IncomingMessage(_))
        .via(
          HdfsFlow.data(
            fs,
            SyncStrategyFactory.none,
            RotationStrategyFactory.time(500.milliseconds),
            HdfsWritingSettings()
          )
        )
        .toMat(Sink.seq)(Keep.both)
        .run()

      system.scheduler.scheduleOnce(1500.milliseconds)(cancellable.cancel()) // cancel within 1500 milliseconds
      val logs = Await.result(resF, Duration.Inf)
      verifyOutputFileSize(fs, logs)
      List(3, 4) should contain(logs.size)
    }

    "should use no rotation and produce one file" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategyFactory.none,
        RotationStrategyFactory.none,
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => books.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs.size shouldEqual 1

      val files = getFiles(fs)
      files.size shouldEqual logs.size
      files.head.getLen shouldEqual books.map(_.toArray.length).sum
    }

    "kafka-example - store data with passThrough" in {
      //#define-kafka-classes
      case class Book(title: String)
      case class KafkaOffset(offset: Int)
      case class KafkaMessage(book: Book, offset: KafkaOffset)
      //#define-kafka-classes

      //#kafka-example
      // We're going to pretend we got messages from kafka.
      // After we've written them to HDFS, we want
      // to commit the offset to Kafka
      val messagesFromKafka = List(
        KafkaMessage(Book("Akka Concurrency"), KafkaOffset(0)),
        KafkaMessage(Book("Akka in Action"), KafkaOffset(1)),
        KafkaMessage(Book("Effective Akka"), KafkaOffset(2)),
        KafkaMessage(Book("Learning Scala"), KafkaOffset(3)),
        KafkaMessage(Book("Scala Puzzlers"), KafkaOffset(4)),
        KafkaMessage(Book("Scala for Spark in Production"), KafkaOffset(5))
      )

      var committedOffsets = List[KafkaOffset]()

      def commitToKafka(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val resF = Source(messagesFromKafka)
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          // Transform message so that we can write to hdfs
          IncomingMessage(ByteString(book.title), kafkaMessage.offset)
        }
        .via(
          HdfsFlow.dataWithPassThrough[KafkaOffset](
            fs,
            SyncStrategyFactory.count(50),
            RotationStrategyFactory.count(4),
            HdfsWritingSettings(newLine = true)
          )
        )
        .map { message =>
          message match {
            case WrittenMessage(passThrough, _) =>
              commitToKafka(passThrough)
            case _ => ()
          }
          message
        }
        .collect {
          case rm: RotationMessage => rm
        }
        .runWith(Sink.seq)
      //#kafka-example

      val logs = Await.result(resF, Duration.Inf)
      logs shouldBe Seq(
        RotationMessage("0", 0),
        RotationMessage("1", 1)
      )

      // Make sure all messages was committed to kafka
      assert(List(0, 1, 2, 3, 4, 5) == committedOffsets.map(_.offset))

      verifyOutputFileSize(fs, logs)
      readLogs(fs, logs).flatMap(_.split("\n")) shouldBe messagesFromKafka.map(_.book.title)
    }
  }

  "CompressedDataWriter" should {
    "use file size rotation and produce six files" in {

      //#define-codec
      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)
      //#define-codec

      //#define-compress
      val flow = HdfsFlow.compressed(
        fs,
        SyncStrategyFactory.count(1),
        RotationStrategyFactory.size(0.1, FileUnit.MB),
        codec,
        settings
      )
      //#define-compress
      val content = generateFakeContentWithPartitions(1, FileUnit.MB.byteCount, 30)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldBe Seq(
        RotationMessage("0.deflate", 0),
        RotationMessage("1.deflate", 1),
        RotationMessage("2.deflate", 2),
        RotationMessage("3.deflate", 3),
        RotationMessage("4.deflate", 4),
        RotationMessage("5.deflate", 5)
      )

      verifyOutputFileSize(fs, logs)
      verifyLogsWithCodec(fs, content, logs, codec)
    }

    "use buffer rotation and produce five files" in {
      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)

      val flow = HdfsFlow.compressed(
        fs,
        SyncStrategyFactory.count(1),
        RotationStrategyFactory.count(1),
        codec,
        settings
      )

      val resF = Source
        .fromIterator(() => books.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldBe Seq(
        RotationMessage("0.deflate", 0),
        RotationMessage("1.deflate", 1),
        RotationMessage("2.deflate", 2),
        RotationMessage("3.deflate", 3),
        RotationMessage("4.deflate", 4)
      )

      verifyOutputFileSize(fs, logs)
      verifyLogsWithCodec(fs, books, logs, codec)
    }

    "should use no rotation and produce one file" in {
      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)

      val flow = HdfsFlow.compressed(
        fs,
        SyncStrategyFactory.none,
        RotationStrategyFactory.none,
        codec,
        settings
      )

      val content = generateFakeContentWithPartitions(1, FileUnit.MB.byteCount, 30)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldEqual Seq(
        RotationMessage("0.deflate", 0)
      )

      verifyOutputFileSize(fs, logs)
      verifyLogsWithCodec(fs, content, logs, codec)
    }
  }

  "SequenceWriter" should {
    "use file size rotation and produce six files without a compression" in {
      //#define-sequence
      val flow = HdfsFlow.sequence(
        fs,
        SyncStrategyFactory.none,
        RotationStrategyFactory.size(1, FileUnit.MB),
        settings,
        classOf[Text],
        classOf[Text]
      )
      //#define-sequence

      // half MB data becomes more when it is sequence
      val content = generateFakeContentForSequence(0.5, FileUnit.MB.byteCount)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)

      verifyOutputFileSize(fs, logs)
      verifySequenceFile(fs, content, logs)
    }

    "use file size rotation and produce six files with a compression" in {
      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)

      //#define-sequence-compressed
      val flow = HdfsFlow.sequence(
        fs,
        SyncStrategyFactory.none,
        RotationStrategyFactory.size(1, FileUnit.MB),
        CompressionType.BLOCK,
        codec,
        settings,
        classOf[Text],
        classOf[Text]
      )
      //#define-sequence-compressed

      // half MB data becomes more when it is sequence
      val content = generateFakeContentForSequence(0.5, FileUnit.MB.byteCount)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)

      verifyOutputFileSize(fs, logs)
      verifySequenceFile(fs, content, logs)
    }

    "use buffer rotation and produce five files" in {
      val flow = HdfsFlow.sequence(
        fs,
        SyncStrategyFactory.none,
        RotationStrategyFactory.count(1),
        settings,
        classOf[Text],
        classOf[Text]
      )

      val content = booksForSequenceWriter

      val resF = Source
        .fromIterator(() => content.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)

      logs.size shouldEqual 5
      verifyOutputFileSize(fs, logs)
      verifySequenceFile(fs, content, logs)
    }

    "should use no rotation and produce one file" in {
      val flow = HdfsFlow.sequence(
        fs,
        SyncStrategyFactory.none,
        RotationStrategyFactory.none,
        settings,
        classOf[Text],
        classOf[Text]
      )

      // half MB data becomes more when it is sequence
      val content = generateFakeContentForSequence(0.5, FileUnit.MB.byteCount)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .map(IncomingMessage(_))
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)

      logs.size shouldEqual 1
      verifyOutputFileSize(fs, logs)
      verifySequenceFile(fs, content, logs)
    }
  }

  override protected def afterEach(): Unit = {
    fs.delete(new Path(destionation), true)
    fs.delete(settings.pathGenerator(0, 0).getParent, true)
    ()
  }

  override protected def beforeAll(): Unit =
    setupCluster()

  override protected def afterAll(): Unit = {
    fs.close()
    hdfsCluster.shutdown()
  }

  private def setupCluster(): Unit = {
    val baseDir = new File(getTestDir, "miniHDFS")
    val conf = new HdfsConfiguration
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.nameNodePort(54310).format(true).build()
    hdfsCluster.waitClusterUp()
  }

  private def documentation(): Unit = {
    //#define-generator
    val pathGenerator =
      FilePathGenerator.create(
        (rotationCount: Long, timestamp: Long) => s"/tmp/alpakka/$rotationCount-$timestamp"
      )
    //#define-generator
    //#define-settings
    val settings =
      HdfsWritingSettings(overwrite = true, newLine = false, pathGenerator = pathGenerator)
    //#define-settings
  }
}
