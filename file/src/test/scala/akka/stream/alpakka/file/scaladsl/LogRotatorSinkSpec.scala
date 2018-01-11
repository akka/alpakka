/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import java.nio.channels.NonWritableChannelException
import java.nio.file._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer}
import akka.testkit.TestKit
import akka.util.ByteString
import com.google.common.jimfs.{Configuration, Jimfs}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object LogRotatorSinkSpec {

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = ActorMaterializer()
    import system.dispatcher

    // #size
    val fileSizeRotationFunction = () => {
      val max = 10 * 1024 * 1024
      var size: Long = max
      (element: ByteString) =>
        {
          if (size + element.size > max) {
            val path = Files.createTempFile("out-", ".log")
            size = element.size
            Some(path)
          } else {
            size += element.size
            None
          }
        }
    }

    val sizeRotatorSink: Sink[ByteString, Future[Done]] =
      LogRotatorSink(fileSizeRotationFunction)
    // #size

    val fileSizeCompletion = Source(Seq("test1", "test2", "test3", "test4", "test5", "test6").toList)
      .map(ByteString(_))
      .runWith(sizeRotatorSink)

    // #time
    val destinationDir = FileSystems.getDefault.getPath("/tmp")
    val formatter = DateTimeFormatter.ofPattern("'stream-'yyyy-MM-dd_HH'.log'")

    val timeBasedRotationFunction = () => {
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
      LogRotatorSink(timeBasedRotationFunction)
    // #time

    val timeBaseCompletion = Source(Seq("test1", "test2", "test3", "test4", "test5", "test6").toList)
      .map(ByteString(_))
      .runWith(timeBasedSink)

    /*
    // #sample
    val pathGeneratorFunction: () => ByteString => Option[Path] = ???

    // #sample
     */
    val pathGeneratorFunction: () => ByteString => Option[Path] = timeBasedRotationFunction
    // #sample
    val completion = Source(Seq("test1", "test2", "test3", "test4", "test5", "test6").toList)
      .map(ByteString(_))
      .runWith(LogRotatorSink(pathGeneratorFunction))
    // #sample

    Future
      .sequence(Seq(timeBaseCompletion, fileSizeCompletion, completion))
      .onComplete(_ => system.terminate())
  }

}

class LogRotatorSinkSpec
    extends TestKit(ActorSystem("LogRotatorSinkSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    fs.close()
  }

  val settings = ActorMaterializerSettings(system).withDispatcher("akka.actor.default-dispatcher")
  implicit val materializer = ActorMaterializer(settings)
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

  def fileLengthFunctionFactory(): (() => (ByteString) => Option[Path], () => Seq[Path]) = {
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
    "write lines to a single file" in {
      var files = Seq.empty[Path]
      val testFunction = () => {
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
      val completion = Source(testByteStrings).runWith(LogRotatorSink(testFunction))
      Await.result(completion, 3.seconds)

      val (contents, sizes) = readUpFilesAndSizesThenClean(files)
      sizes should contain theSameElementsAs Seq(6006L)
      contents should contain theSameElementsAs Seq(TestLines.mkString(""))
    }
    "write lines to multiple files due to filesize" in {
      val (testFunction, files) = fileLengthFunctionFactory()
      val completion =
        Source(testByteStrings).runWith(LogRotatorSink(testFunction))

      Await.result(completion, 3.seconds)

      val (contents, sizes) = readUpFilesAndSizesThenClean(files())
      sizes should contain theSameElementsAs Seq(2002L, 2002L, 2002L)
      contents should contain theSameElementsAs TestLines.sliding(2, 2).map(_.mkString("")).toList
    }
    "upstream fail before first file creation" in {
      val (testFunction, files) = fileLengthFunctionFactory()
      val (probe, completion) =
        TestSource.probe[ByteString].toMat(LogRotatorSink(testFunction))(Keep.both).run()

      val ex = new Exception("my-exception")
      probe.sendError(ex)
      the[Exception] thrownBy Await.result(completion, 3.seconds) shouldBe ex
      files() shouldBe empty
    }

    "upstream fail after first file creation" in {
      val (testFunction, files) = fileLengthFunctionFactory()
      val (probe, completion) =
        TestSource.probe[ByteString].toMat(LogRotatorSink(testFunction))(Keep.both).run()

      val ex = new Exception("my-exception")
      probe.sendNext(ByteString("test"))
      probe.sendError(ex)
      the[Exception] thrownBy Await.result(completion, 3.seconds) shouldBe ex
      files().size shouldBe 1
      readUpFilesAndSizesThenClean(files())
    }
    "function fail on path creation" in {
      val ex = new Exception("my-exception")
      val testFunction = () => { (x: ByteString) =>
        {
          throw ex
        }
      }
      val (probe, completion) =
        TestSource.probe[ByteString].toMat(LogRotatorSink(testFunction))(Keep.both).run()
      probe.sendNext(ByteString("test"))
      the[Exception] thrownBy Await.result(completion, 3.seconds) shouldBe ex
    }

    "downstream fail on file write" in {
      val path = Files.createTempFile(fs.getPath("/"), "test", ".log")
      val testFunction = () => { (x: ByteString) =>
        {
          Option(path)
        }
      }
      val (probe, completion) =
        TestSource.probe[ByteString].toMat(LogRotatorSink(testFunction, Set(StandardOpenOption.READ)))(Keep.both).run()
      probe.sendNext(ByteString("test"))
      probe.sendNext(ByteString("test"))
      probe.expectCancellation()
      the[Exception] thrownBy Await.result(completion, 3.seconds) shouldBe a[NonWritableChannelException]
    }

  }

  def readUpFilesAndSizesThenClean(files: Seq[Path]): (Seq[String], Seq[Long]) = {
    var sizes = Seq.empty[Long]
    var data = Seq.empty[String]
    files.foreach { path =>
      sizes = sizes :+ Files.size(path)
      data = data :+ new String(Files.readAllBytes(path))
      Files.delete(path)
    }
    (data, sizes)
  }
}
