/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.file._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import akka.Done
import akka.actor.ActorSystem
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Compression, FileIO, Flow, Keep, Sink, Source}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestKit
import akka.util.ByteString
import com.google.common.jimfs.{Configuration, Jimfs}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class LogRotatorSinkSpec
    extends TestKit(ActorSystem("LogRotatorSinkSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with LogCapturing {

  implicit val patience: PatienceConfig = PatienceConfig(5.seconds, 100.millis)

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    fs.close()
  }

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

  private def fileLengthTriggerCreator(): (() => ByteString => Option[Path], () => Seq[Path]) = {
    var files = Seq.empty[Path]
    val testFunction = () => {
      val max = 2002
      var size: Long = max
      (element: ByteString) => {
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

    "complete when consuming an empty source" in assertAllStagesStopped {
      val triggerCreator: () => ByteString => Option[Path] = () => {
        element: ByteString => fail("trigger creator should not be called")
      }

      val rotatorSink: Sink[ByteString, Future[Done]] =
        LogRotatorSink(triggerCreator)

      val completion = Source.empty[ByteString].runWith(rotatorSink)
      completion.futureValue shouldBe Done
    }

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
        (_: ByteString) => {
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

    "work for stream-based rotation " in assertAllStagesStopped {
      // #stream
      val destinationDir = FileSystems.getDefault.getPath("/tmp")

      val streamBasedTriggerCreator: () => ((String, String)) => Option[Path] = () => {
        var currentFilename: Option[String] = None
        (element: (String, String)) => {
          if (currentFilename.contains(element._1)) {
            None
          } else {
            currentFilename = Some(element._1)
            Some(destinationDir.resolve(element._1))
          }
        }
      }

      val timeBasedSink: Sink[(String, String), Future[Done]] =
        LogRotatorSink.withTypedSinkFactory(
          streamBasedTriggerCreator,
          (path: Path) =>
            Flow[(String, String)]
              .map { case (_, data) => ByteString(data) }
              .via(Compression.gzip)
              .toMat(FileIO.toPath(path))(Keep.right)
        )
      // #stream

      val timeBaseCompletion = Source(
        immutable.Seq(
          ("stream1", "test1"),
          ("stream1", "test2"),
          ("stream1", "test3"),
          ("stream2", "test4"),
          ("stream2", "test5"),
          ("stream2", "test6")
        )
      ).runWith(timeBasedSink)

      timeBaseCompletion.futureValue shouldBe Done

    }

    "write lines to a single file" in assertAllStagesStopped {
      var files = Seq.empty[Path]
      val triggerFunctionCreator = () => {
        var fileName: String = null
        (element: ByteString) => {
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

    "correctly close sinks" in assertAllStagesStopped {
      val test = (1 to 3).map(_.toString).toList
      var out = Seq.empty[String]
      def add(e: ByteString): Unit = {
        out = out :+ e.utf8String
      }

      val completion =
        Source(test.map(ByteString.apply)).runWith(
          LogRotatorSink.withSinkFactory[Unit, Done](
            triggerGeneratorCreator = () => (_: ByteString) => Some({}),
            sinkFactory = (_: Unit) =>
              Flow[ByteString].toMat(new StrangeSlowSink[ByteString](add, 100.millis, 200.millis))(Keep.right)
          )
        )

      Await.result(completion, 3.seconds)
      out should contain theSameElementsAs test
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
      val triggerFunctionCreator = () => {
        (x: ByteString) => {
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
      val triggerFunctionCreator = () => {
        (x: ByteString) => {
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

      val exception = intercept[Exception] {
        Await.result(completion, 3.seconds)
      }

      exactly(
        1,
        List(exception, // Akka 2.5 throws nio exception directly
             exception.getCause) // Akka 2.6 wraps nio exception in a akka.stream.IOOperationIncompleteException
      ) shouldBe a[java.nio.channels.NonWritableChannelException]
    }

  }

  "downstream fail on exception in sink" in assertAllStagesStopped {
    val path = Files.createTempFile(fs.getPath("/"), "test", ".log")
    val triggerFunctionCreator = () => {
      (x: ByteString) => {
        Option(path)
      }
    }
    val (probe, completion) =
      TestSource
        .probe[ByteString]
        .toMat(
          LogRotatorSink.withSinkFactory(
            triggerGeneratorCreator = triggerFunctionCreator,
            sinkFactory = (_: Path) =>
              Flow[ByteString]
                .map { data =>
                  if (data.utf8String == "test") throw new IllegalArgumentException("The data is broken")
                  data
                }
                .toMat(Sink.ignore)(Keep.right)
          )
        )(Keep.both)
        .run()

    probe.sendNext(ByteString("test"))
    probe.sendNext(ByteString("test"))
    probe.expectCancellation()

    val exception = intercept[Exception] {
      Await.result(completion, 3.seconds)
    }

    exactly(
      1,
      List(exception, // Akka 2.5 throws nio exception directly
           exception.getCause) // Akka 2.6 wraps nio exception in a akka.stream.IOOperationIncompleteException
    ) shouldBe a[IllegalArgumentException]
  }

  private def readUpFilesAndSizesThenClean(files: Seq[Path]): (Seq[String], Seq[Long]) = {
    val (bytes, sizes) = readUpFileBytesAndSizesThenClean(files)
    (bytes.map(_.utf8String), sizes)
  }

  private def readUpFileBytesAndSizesThenClean(files: Seq[Path]): (Seq[ByteString], Seq[Long]) = {
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

  class StrangeSlowSink[A](callback: A => Unit, waitBeforePull: FiniteDuration, waitAfterComplete: FiniteDuration)
      extends GraphStageWithMaterializedValue[SinkShape[A], Future[Done]] {
    val in: Inlet[A] = Inlet("StrangeSlowSink.in")
    override val shape: SinkShape[A] = SinkShape(in)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
      val promise = Promise[Done]()
      val logic = new GraphStageLogic(shape) {
        override def preStart(): Unit = {
          Await.result(Future(Thread.sleep(waitBeforePull.toMillis)), 1.minute)
          pull(in)
        }
        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              callback(grab(in))
              pull(in)
            }

            override def onUpstreamFinish(): Unit = {
              Await.result(Future(Thread.sleep(waitAfterComplete.toMillis)), 1.minute)
              promise.success(Done.done())
              super.onUpstreamFinish()
            }
          }
        )
      }

      (logic, promise.future)
    }
  }
}
