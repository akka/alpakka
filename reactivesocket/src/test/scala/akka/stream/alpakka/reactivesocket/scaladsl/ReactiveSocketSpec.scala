package akka.stream.alpakka.reactivesocket.scaladsl

import akka.stream.scaladsl._
import io.reactivesocket.util.PayloadImpl
import org.scalatest.WordSpec

import scala.concurrent.duration._

class ReactiveSocketSpec extends WordSpec {

  "reactive socket API" should {

    "support server and client" in {

      import io.reactivesocket.transport.tcp.client.TcpTransportClient
      import io.reactivesocket.transport.tcp.server.TcpTransportServer

      val binding = ReactiveSocketServer(TcpTransportServer.create())
          .toMat(Sink.foreach {
            _.flow.runWith(Source.repeat(new PayloadImpl("Pong")), Sink.foreach(println))
          })(Keep.left)
          .run()

      implicit val rs = ReactiveSocket(TcpTransportClient.create(binding.localAddress))

      ReactiveSocketClient.stream(new PayloadImpl("One time ping")).runForeach(println)

      Source.tick(1.second, 1.second, new PayloadImpl("Continuous ping")).via(ReactiveSocketClient.channel()).runForeach(println)

    }

    "support local transport" in {

      import io.reactivesocket.local.LocalServer
      import io.reactivesocket.local.LocalClient

      val address = "test-local-server"

      val binding: Source[ReactiveSocketServer.IncomingConnection, ReactiveSocketServer.Binding] = ReactiveSocketServer(LocalServer.create(address))

      implicit val rs = ReactiveSocket(LocalClient.create(address))

      Source.tick(1.second, 1.second, new PayloadImpl("Hi")).via(ReactiveSocketClient.channel()).runForeach(println)
    }

    "support aeron transport" in {

      import io.reactivesocket.aeron.internal._
      import io.reactivesocket.aeron.internal.reactivestreams._
      import io.reactivesocket.aeron.server.AeronTransportServer
      import io.reactivesocket.aeron.client.AeronTransportClient

      // server

      // aeron config, which should be hidden from the user
      val aeronWrapper = new DefaultAeronWrapper()
      val serverManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790)
      val serverEventLoop = new SingleThreadedEventLoop("server")
      val server = new AeronTransportServer(aeronWrapper, serverManagementSocketAddress, serverEventLoop)

      // akka streams reactive socket
      val binding = ReactiveSocketServer(new AeronTransportServer(aeronWrapper, serverManagementSocketAddress, serverEventLoop))

      // client

      // aeron config, which should be hidden from the user
      val clientManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790)
      val clientEventLoop = new SingleThreadedEventLoop("client")

      val receiveAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790)
      val sendAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790)

      val config = AeronClientChannelConnector
        .AeronClientConfig.create(
        receiveAddress,
        sendAddress,
        Constants.CLIENT_STREAM_ID,
        Constants.SERVER_STREAM_ID,
        clientEventLoop)

      val connector = AeronClientChannelConnector
        .create(aeronWrapper,
          clientManagementSocketAddress,
          clientEventLoop)

      // akka streams reactive socket
      implicit val rs = ReactiveSocket(new AeronTransportClient(connector, config))

      Source.tick(1.second, 1.second, new PayloadImpl("Hi")).via(ReactiveSocketClient.channel()).runForeach(println)
    }

  }

}
