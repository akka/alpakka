/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.io.{File, IOException}
import java.nio.file.Files

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.unixdomainsocket.scaladsl.UnixDomainSocket
import akka.stream.scaladsl._
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

  "A Unix Domain Socket" should {
    "receive what is sent" in {
      //#binding
      val file: java.io.File = // ...
        //#binding
        Files.createTempFile("UnixDomainSocketSpec1", ".sock").toFile
      file.delete()
      file.deleteOnExit()

      //#binding
      val binding: Future[UnixDomainSocket.ServerBinding] =
        UnixDomainSocket().bindAndHandle(Flow.fromFunction(identity), file)
      //#binding

      //#outgoingConnection
      binding.flatMap { connection =>
        val sendBytes = ByteString("Hello")
        val result: Future[ByteString] =
          Source
            .single(sendBytes)
            .via(UnixDomainSocket().outgoingConnection(file))
            .runWith(Sink.head)
        //#outgoingConnection
        result
          .map(receiveBytes => assert(receiveBytes == sendBytes))
          .flatMap {
            case `succeed` => connection.unbind().map(_ => succeed)
            case failedAssertion => failedAssertion
          }
      //#outgoingConnection
      }
      //#outgoingConnection
    }

    "allow the client to close the connection" in {
      val file = Files.createTempFile("UnixDomainSocketSpec2", ".sock").toFile
      file.delete()
      file.deleteOnExit()

      val sendBytes = ByteString("Hello")

      val binding = UnixDomainSocket().bindAndHandle(Flow[ByteString].map(identity), file)

      binding.flatMap { connection =>
        Source
          .single(sendBytes)
          .via(UnixDomainSocket().outgoingConnection(new UnixSocketAddress(file), halfClose = false))
          .runWith(Sink.head)
          .flatMap { _ =>
            connection.unbind().map(_ => succeed)
          }
      }
    }

    "close the server once the client is also closed" in {
      val file = Files.createTempFile("UnixDomainSocketSpec3", ".sock").toFile
      file.delete()
      file.deleteOnExit()

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
          .runWith(Sink.head)
          .flatMap { _ =>
            connection.unbind().map(_ => succeed)
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
          .viaMat(UnixDomainSocket().outgoingConnection(new File("/thisshouldnotexist")))(Keep.right)
          .to(Sink.ignore)
          .run()

      connection.failed.map {
        case _: IOException => succeed
      }
    }

    "be able to materialize outgoing connection flow more than once" in {
      @inline def materialize(flow: Flow[ByteString, ByteString, Any]) =
        Source.single(ByteString("Hello")).via(flow).runWith(Sink.head)

      val file = Files.createTempFile("UnixDomainSocketSpec4", ".sock").toFile
      file.delete()
      file.deleteOnExit()

      val binding: Future[UnixDomainSocket.ServerBinding] =
        UnixDomainSocket().bindAndHandle(Flow.fromFunction(identity), file, halfClose = true)

      val flow = UnixDomainSocket().outgoingConnection(file)

      binding.flatMap { bound =>
        Future
          .sequence(List(materialize(flow), materialize(flow)))
          .flatMap(r => bound.unbind.map(_ => succeed).recover { case _ => succeed })
      }

    }

  }
}
