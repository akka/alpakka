/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.channels.NonWritableChannelException
import java.nio.file._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.stream.scaladsl.{Compression, FileIO, Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import akka.util.ByteString
import com.google.common.jimfs.{Configuration, Jimfs}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class LogRotatorSinkSpec
    extends TestKit(ActorSystem("LogRotatorSinkSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val patience: PatienceConfig = PatienceConfig(5.seconds, 100.millis)

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    fs.close()
  }

  val settings = ActorMaterializerSettings(system).withDispatcher("akka.actor.default-dispatcher")
  implicit val materializer = ActorMaterializer(settings)
  implicit val ec: ExecutionContext = system.dispatcher
  val fs = Jimfs.newFileSystem("LogRotatorSinkSpec", Configuration.unix())

  val TestLines = {
    val buffer = ListBuffer[String]()
    buffer.append("a" * 1000 + "\n")
    buffer.append("b" * 1000 + "\n")
    buffer.append("c" * 1000 + "\n")
    buffer.append("d" * 1000 + "\n")
    buffer.append("e" * 1000 + "\n")
    buffer.append("f" * 1000 + "\n")
    buffer.toList
  }

  def fileLengthTriggerCreator(): (() => ByteString => Option[Path], () => Seq[Path]) = {
    var files = Seq.empty[Path]
    val testFunction = () => {
      val max = 2002
      var size: Long = max
      (element: ByteString) =>
        {
          if (size + element.size > max) {
            val path = Files.createTempFile(fs.getPath("/"), "test", ".log")
            files :+= path
            size = element.size
            Option(path)
          } else {
            size += element.size
            Option.empty[Path]
          }
        }
    }
    val listFiles = () => files
    (testFunction, listFiles)
  }

  val testByteStrings = TestLines.map(ByteString(_))

  "LogRotatorSink" must {
    "work for size-based rotation " in assertAllStagesStopped {
      // #size
      import akka.stream.alpakka.file.scaladsl.LogRotatorSink

      val fileSizeTriggerCreator: () => ByteString => Option[Path] = () => {
        val max = 10 * 1024 * 1024
        var size: Long = max
        element: ByteString =>
          if (size + element.size > max) {
            val path = Files.createTempFile("out-", ".log")
            size = element.size
            Some(path)
          } else {
            size += element.size
            None
          }
      }

      val sizeRotatorSink: Sink[ByteString, Future[Done]] =
        LogRotatorSink(fileSizeTriggerCreator)
      // #size

      val fileSizeCompletion = Source(immutable.Seq("test1", "test2", "test3", "test4", "test5", "test6"))
        .map(ByteString(_))
        .runWith(sizeRotatorSink)

      fileSizeCompletion.futureValue shouldBe Done
    }

    "work for time-based rotation " in assertAllStagesStopped {
      // #time
      val destinationDir = FileSystems.getDefault.getPath("/tmp")
      val formatter = DateTimeFormatter.ofPattern("'stream-'yyyy-MM-dd_HH'.log'")

      val timeBasedTriggerCreator: () => ByteString => Option[Path] = () => {
        var currentFilename: Option[String] = None
        (_: ByteString) =>
          {
            val newName = LocalDateTime.now().format(formatter)
            if (currentFilename.contains(newName)) {
              None
            } else {
              currentFilename = Some(newName)
              Some(destinationDir.resolve(newName))
            }
          }
      }

      val timeBasedSink: Sink[ByteString, Future[Done]] =
        LogRotatorSink(timeBasedTriggerCreator)
      // #time

      val timeBaseCompletion = Source(immutable.Seq("test1", "test2", "test3", "test4", "test5", "test6"))
        .map(ByteString(_))
        .runWith(timeBasedSink)

      /*
      // #sample
      val triggerFunctionCreator: () => ByteString => Option[Path] = ???

      // #sample
       */
      val triggerFunctionCreator: () => ByteString => Option[Path] = timeBasedTriggerCreator
      // #sample
      val completion = Source(immutable.Seq("test1", "test2", "test3", "test4", "test5", "test6"))
        .map(ByteString(_))
        .runWith(LogRotatorSink(triggerFunctionCreator))
      // #sample
      completion.futureValue shouldBe Done

      timeBaseCompletion.futureValue shouldBe Done

    }

    "write lines to a single file" in assertAllStagesStopped {
      var files = Seq.empty[Path]
      val triggerFunctionCreator = () => {
        var fileName: String = null
        (element: ByteString) =>
          {
            if (fileName == null) {
              val path = Files.createTempFile(fs.getPath("/"), "test", ".log")
              files :+= path
              fileName = path.toString
              Some(path)
            } else {
              None
            }
          }
      }
      val completion = Source(testByteStrings).runWith(LogRotatorSink(triggerFunctionCreator))
      Await.result(completion, 3.seconds)

      val (contents, sizes) = readUpFilesAndSizesThenClean(files)
      sizes should contain theSameElementsAs Seq(6006L)
      contents should contain theSameElementsAs Seq(TestLines.mkString(""))
    }

    "write lines to multiple files due to filesize" in assertAllStagesStopped {
      val (triggerFunctionCreator, files) = fileLengthTriggerCreator()
      val completion =
        Source(testByteStrings).runWith(LogRotatorSink(triggerFunctionCreator))

      Await.result(completion, 3.seconds)

      val (contents, sizes) = readUpFilesAndSizesThenClean(files())
      sizes should contain theSameElementsAs Seq(2002L, 2002L, 2002L)
      contents should contain theSameElementsAs TestLines.sliding(2, 2).map(_.mkString("")).toList
    }

    "write compressed lines to multiple targets" in assertAllStagesStopped {
      val (triggerFunctionCreator, files) = fileLengthTriggerCreator()
      val source = Source(testByteStrings)
      // #sample

      // GZip compressing the data written
      val completion =
        source
          .runWith(
            LogRotatorSink.withSinkFactory(
              triggerFunctionCreator,
              (path: Path) =>
                Flow[ByteString]
                  .via(Compression.gzip)
                  .toMat(FileIO.toPath(path))(Keep.right)
            )
          )
      // #sample

      completion.futureValue shouldBe Done

      val (contents, sizes) = readUpFileBytesAndSizesThenClean(files())
      sizes should contain theSameElementsAs Seq(51L, 51L, 51L)
      val uncompressed = Future.sequence(contents.map { c =>
        Source.single(c).via(Compression.gunzip()).map(_.utf8String).runWith(Sink.head)
      })
      uncompressed.futureValue should contain theSameElementsAs TestLines.sliding(2, 2).map(_.mkString("")).toList
    }

    "upstream fail before first file creation" in assertAllStagesStopped {
      val (triggerFunctionCreator, files) = fileLengthTriggerCreator()
      val (probe, completion) =
        TestSource.probe[ByteString].toMat(LogRotatorSink(triggerFunctionCreator))(Keep.both).run()

      val ex = new Exception("my-exception")
      probe.sendError(ex)
      the[Exception] thrownBy Await.result(completion, 3.seconds) shouldBe ex
      files() shouldBe empty
    }

    "upstream fail after first file creation" in assertAllStagesStopped {
      val (triggerFunctionCreator, files) = fileLengthTriggerCreator()
      val (probe, completion) =
        TestSource.probe[ByteString].toMat(LogRotatorSink(triggerFunctionCreator))(Keep.both).run()

      val ex = new Exception("my-exception")
      probe.sendNext(ByteString("test"))
      probe.sendError(ex)
      the[Exception] thrownBy Await.result(completion, 3.seconds) shouldBe ex
      files().size shouldBe 1
      readUpFilesAndSizesThenClean(files())
    }

    "function fail on path creation" in assertAllStagesStopped {
      val ex = new Exception("my-exception")
      val triggerFunctionCreator = () => { (x: ByteString) =>
        {
          throw ex
        }
      }
      val (probe, completion) =
        TestSource.probe[ByteString].toMat(LogRotatorSink(triggerFunctionCreator))(Keep.both).run()
      probe.sendNext(ByteString("test"))
      the[Exception] thrownBy Await.result(completion, 3.seconds) shouldBe ex
    }

    "downstream fail on file write" in assertAllStagesStopped {
      val path = Files.createTempFile(fs.getPath("/"), "test", ".log")
      val triggerFunctionCreator = () => { (x: ByteString) =>
        {
          Option(path)
        }
      }
      val (probe, completion) =
        TestSource
          .probe[ByteString]
          .toMat(LogRotatorSink(triggerFunctionCreator, Set(StandardOpenOption.READ)))(Keep.both)
          .run()
      probe.sendNext(ByteString("test"))
      probe.sendNext(ByteString("test"))
      probe.expectCancellation()
      the[Exception] thrownBy Await.result(completion, 3.seconds) shouldBe a[NonWritableChannelException]
    }

  }

  def readUpFilesAndSizesThenClean(files: Seq[Path]): (Seq[String], Seq[Long]) = {
    val (bytes, sizes) = readUpFileBytesAndSizesThenClean(files)
    (bytes.map(_.utf8String), sizes)
  }

  def readUpFileBytesAndSizesThenClean(files: Seq[Path]): (Seq[ByteString], Seq[Long]) = {
    var sizes = Seq.empty[Long]
    var data = Seq.empty[ByteString]
    files.foreach { path =>
      sizes = sizes :+ Files.size(path)
      data = data :+ ByteString(Files.readAllBytes(path))
      Files.delete(path)
      ()
    }
    (data, sizes)
  }
}
