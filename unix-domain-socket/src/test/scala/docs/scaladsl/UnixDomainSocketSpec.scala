/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.io.{File, IOException}
import java.nio.file.Files

import akka.Done
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.alpakka.unixdomainsocket.scaladsl.UnixDomainSocket
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.testkit._
import akka.util.ByteString
import jnr.unixsocket.UnixSocketAddress
import org.scalatest._

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class UnixDomainSocketSpec
    extends TestKit(ActorSystem("UnixDomainSocketSpec"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  implicit val ma: ActorMaterializer = ActorMaterializer()

  private val dir = Files.createTempDirectory("UnixDomainSocketSpec")

  "A Unix Domain Socket" should {
    "receive some bytes" in {
      //#binding
      val file: java.io.File = // ...
        //#binding
        dir.resolve("sock1").toFile

      val received = Promise[ByteString]

      val serverSideFlow = Flow[ByteString]
        .buffer(1, OverflowStrategy.backpressure)
        .wireTap(bytes => received.success(bytes))

      //#binding
      val binding: Future[UnixDomainSocket.ServerBinding] =
        UnixDomainSocket().bindAndHandle(serverSideFlow, file)
      //#binding

      //#outgoingConnection
      binding.flatMap { _ => // connection
        val sendBytes = ByteString("Hello")
        Source
          .single(sendBytes)
          .via(UnixDomainSocket().outgoingConnection(file))
          .runWith(Sink.ignore)
        //#outgoingConnection
        received.future.map(receiveBytes => assert(receiveBytes == sendBytes))
        //#outgoingConnection
      }
      //#outgoingConnection
    }

    "send and receive more ten times the size of a buffer" ignore {
      val BufferSizeBytes = 64 * 1024

      val file = dir.resolve("sock2").toFile

      val binding: Future[UnixDomainSocket.ServerBinding] =
        UnixDomainSocket().bindAndHandle(Flow.fromFunction(identity), file, halfClose = true)

      binding.flatMap { connection =>
        val sendBytes = ByteString(Array.ofDim[Byte](BufferSizeBytes * 10))
        val result: Future[ByteString] =
          Source
            .single(sendBytes)
            .via(UnixDomainSocket().outgoingConnection(file))
            .runWith(Sink.fold(ByteString.empty) { case (acc, b) => acc ++ b })
        result
          .map(receiveBytes => assert(receiveBytes == sendBytes))
          .flatMap {
            case `succeed` => connection.unbind().map(_ => succeed)
            case failedAssertion => failedAssertion
          }
      }
    }

    "allow the client to close the connection" in {
      val file = dir.resolve("sock3").toFile

      val sendBytes = ByteString("Hello")

      val binding =
        UnixDomainSocket().bindAndHandle(Flow[ByteString].delay(5.seconds), file)

      binding.flatMap { connection =>
        Source
          .single(sendBytes)
          .via(UnixDomainSocket().outgoingConnection(new UnixSocketAddress(file), halfClose = false))
          .runWith(Sink.headOption)
          .flatMap {
            case e if e.isEmpty => connection.unbind().map(_ => succeed)
          }
      }
    }

    "close the server once the client is also closed" in {
      val file = dir.resolve("sock4").toFile

      val sendBytes = ByteString("Hello")
      val receiving = Promise[Done]

      val binding =
        UnixDomainSocket().bindAndHandle(
          Flow.fromFunction[ByteString, ByteString](identity).wireTap(_ => receiving.success(Done)).delay(1.second),
          file,
          halfClose = true
        )

      binding.flatMap { connection =>
        Source
          .tick(0.seconds, 1.second, sendBytes)
          .takeWhile(_ => !receiving.isCompleted)
          .via(UnixDomainSocket().outgoingConnection(file))
          .runWith(Sink.headOption)
          .flatMap {
            case e if e.nonEmpty => connection.unbind().map(_ => succeed)
          }
      }
    }

    "not be able to bind to a non-existent file" in {
      val binding =
        UnixDomainSocket().bindAndHandle(Flow.fromFunction(identity), new File("/thisshouldnotexist"))

      binding.failed.map {
        case _: IOException => succeed
      }
    }

    "not be able to connect to a non-existent file" in {
      val connection =
        Source
          .single(ByteString("hi"))
          .via(UnixDomainSocket().outgoingConnection(new File("/thisshouldnotexist")))
          .runWith(Sink.head)

      connection.failed.map {
        case _: IOException => succeed
      }
    }
  }
}
