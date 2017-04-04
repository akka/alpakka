package akka.stream.alpakka.udp

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.{IO, Udp}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.udp.sink.{UdpMessage, UdpSink}
import akka.stream.scaladsl.Source
import akka.testkit.{TestActorRef, TestKit}
import akka.util.ByteString
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Milliseconds, Second, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec, WordSpecLike}

import scala.util.{Failure, Success, Try}

object Utils {
  def freePort: Try[Int] = {
    Try {
      val s = new ServerSocket(0)
      try {
        s.getLocalPort
      } catch {
        case any: Throwable ⇒
          throw any
      } finally {
        s.close
      }
    }
  }
}

class UdpSpec extends TestKit(ActorSystem("udp-test"))
  with WordSpecLike
  with Matchers
  with Eventually
  with BeforeAndAfterAll {

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(Span(3, Seconds)),
    interval = scaled(Span(50, Milliseconds)))

  override def afterAll = {
    system.terminate()
  }

  "UDP sink" must {
    "messages sent via the stream" in {

      implicit val materializer = ActorMaterializer()

      Utils.freePort match {
        case Failure(ex) ⇒ fail(ex)
        case Success(port) ⇒
          val messagesToSend = 100
          val receivedCount = new AtomicInteger(0)
          val address = new InetSocketAddress("localhost", port)

          val server = TestActorRef(new Actor {
            override def preStart(): Unit = {
              IO(Udp) ! Udp.Bind(self, address)
            }
            def receive = {
              case Udp.Bound(_) ⇒
                context.become(ready(sender()))
            }
            def ready(socket: ActorRef): Receive = {
              case Udp.Received(data, remote) ⇒
                receivedCount.incrementAndGet()
            }
          })

          Source(1 to messagesToSend)
            .map({ i ⇒ ByteString(s"message $i") })
            .map(UdpMessage(_, address))
            .runWith(new UdpSink)

          eventually {
            receivedCount.intValue() shouldBe messagesToSend
          }
      }
    }
  }

}
