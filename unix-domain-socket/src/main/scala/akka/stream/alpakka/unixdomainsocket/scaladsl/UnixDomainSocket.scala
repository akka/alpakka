/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket
package scaladsl

import java.nio.file.Path

import akka.NotUsed
import akka.actor.{ClassicActorSystemProvider, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream._
import akka.stream.alpakka.unixdomainsocket.impl.UnixDomainSocketImpl
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future
import scala.concurrent.duration.Duration

object UnixDomainSocket extends ExtensionId[UnixDomainSocket] with ExtensionIdProvider {

  /**
   * Get the UnixDomainSocket extension with the classic or new actors API.
   */
  def apply()(implicit system: ClassicActorSystemProvider): UnixDomainSocket = super.apply(system)

  /**
   * Get the UnixDomainSocket extension with the classic actors API.
   */
  override def apply(system: akka.actor.ActorSystem): UnixDomainSocket = super.apply(system)

  override def createExtension(system: ExtendedActorSystem) =
    new UnixDomainSocket(system)

  override def lookup(): ExtensionId[_ <: Extension] =
    UnixDomainSocket

  /**
   * * Represents a successful server binding.
   */
  final case class ServerBinding(localAddress: UnixSocketAddress)(private val unbindAction: () => Future[Unit]) {
    def unbind(): Future[Unit] = unbindAction()
  }

  /**
   * Represents an accepted incoming connection.
   */
  final case class IncomingConnection(localAddress: UnixSocketAddress,
                                      remoteAddress: UnixSocketAddress,
                                      flow: Flow[ByteString, ByteString, NotUsed]) {

    /**
     * Handles the connection using the given flow, which is materialized exactly once and the respective
     * materialized instance is returned.
     *
     * Convenience shortcut for: `flow.join(handler).run()`.
     */
    def handleWith[Mat](handler: Flow[ByteString, ByteString, Mat])(implicit materializer: Materializer): Mat =
      flow.joinMat(handler)(Keep.right).run()
  }

  /**
   * Represents a prospective outgoing Unix Domain Socket connection.
   */
  final case class OutgoingConnection(remoteAddress: UnixSocketAddress, localAddress: UnixSocketAddress)

}

/**
 * Provides Unix Domain Socket functionality to Akka Streams with an interface similar to Akka's Tcp class.
 */
final class UnixDomainSocket(system: ExtendedActorSystem) extends UnixDomainSocketImpl(system) {

  import UnixDomainSocket._

  private implicit val materializer: Materializer = Materializer(system)

  /**
   * Creates a [[UnixDomainSocket.ServerBinding]] instance which represents a prospective Unix Domain Socket
   * server binding on the given `endpoint`.
   *
   * Please note that the startup of the server is asynchronous, i.e. after materializing the enclosing
   * [[akka.stream.scaladsl.RunnableGraph]] the server is not immediately available. Only after the materialized future
   * completes is the server ready to accept client connections.
   *
   * TODO: Support idleTimeout as per Tcp.
   *
   * @param path      The path to listen on
   * @param backlog   Controls the size of the connection backlog
   * @param halfClose
   *                  Controls whether the connection is kept open even after writing has been completed to the accepted
   *                  socket connections.
   *                  If set to true, the connection will implement the socket half-close mechanism, allowing the client to
   *                  write to the connection even after the server has finished writing. The socket is only closed
   *                  after both the client and server finished writing.
   *                  If set to false, the connection will immediately closed once the server closes its write side,
   *                  independently whether the client is still attempting to write. This setting is recommended
   *                  for servers, and therefore it is the default setting.
   */
  override def bind(path: Path,
                    backlog: Int = 128,
                    halfClose: Boolean = false): Source[IncomingConnection, Future[ServerBinding]] =
    super.bind(path, backlog, halfClose)

  /**
   * Creates a [[UnixDomainSocket.ServerBinding]] instance which represents a prospective Unix Socket server binding on the given `endpoint`
   * handling the incoming connections using the provided Flow.
   *
   * Please note that the startup of the server is asynchronous, i.e. after materializing the enclosing
   * [[akka.stream.scaladsl.RunnableGraph]] the server is not immediately available. Only after the returned future
   * completes is the server ready to accept client connections.
   *
   * TODO: Support idleTimeout as per Tcp.
   *
   * @param handler   A Flow that represents the server logic
   * @param path      The path to listen on
   * @param backlog   Controls the size of the connection backlog
   * @param halfClose
   *                  Controls whether the connection is kept open even after writing has been completed to the accepted
   *                  socket connections.
   *                  If set to true, the connection will implement the socket half-close mechanism, allowing the client to
   *                  write to the connection even after the server has finished writing. The socket is only closed
   *                  after both the client and server finished writing.
   *                  If set to false, the connection will immediately closed once the server closes its write side,
   *                  independently whether the client is still attempting to write. This setting is recommended
   *                  for servers, and therefore it is the default setting.
   */
  def bindAndHandle(handler: Flow[ByteString, ByteString, _],
                    path: Path,
                    backlog: Int = 128,
                    halfClose: Boolean = false): Future[ServerBinding] =
    bind(path, backlog, halfClose)
      .to(Sink.foreach { conn: IncomingConnection =>
        conn.flow.join(handler).run()
      })
      .run()

  /**
   * Creates an [[UnixDomainSocket.OutgoingConnection]] instance representing a prospective Unix Domain client connection to the given endpoint.
   *
   * Note that the ByteString chunk boundaries are not retained across the network,
   * to achieve application level chunks you have to introduce explicit framing in your streams,
   * for example using the [[akka.stream.scaladsl.Framing]] stages.
   *
   * TODO: Support idleTimeout as per Tcp.
   *
   * @param remoteAddress The remote address to connect to
   * @param localAddress  Optional local address for the connection
   * @param halfClose
   *                  Controls whether the connection is kept open even after writing has been completed to the accepted
   *                  socket connections.
   *                  If set to true, the connection will implement the socket half-close mechanism, allowing the server to
   *                  write to the connection even after the client has finished writing. The socket is only closed
   *                  after both the client and server finished writing. This setting is recommended for clients and
   *                  therefore it is the default setting.
   *                  If set to false, the connection will immediately closed once the client closes its write side,
   *                  independently whether the server is still attempting to write.
   */
  override def outgoingConnection(
      remoteAddress: UnixSocketAddress,
      localAddress: Option[UnixSocketAddress] = None,
      halfClose: Boolean = true,
      connectTimeout: Duration = Duration.Inf
  ): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    super.outgoingConnection(remoteAddress, localAddress, halfClose, connectTimeout)

  /**
   * Creates an [[UnixDomainSocket.OutgoingConnection]] without specifying options.
   * It represents a prospective Unix Domain client connection to the given endpoint.
   *
   * Note that the ByteString chunk boundaries are not retained across the network,
   * to achieve application level chunks you have to introduce explicit framing in your streams,
   * for example using the [[akka.stream.scaladsl.Framing]] stages.
   */
  def outgoingConnection(path: Path): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    super.outgoingConnection(UnixSocketAddress(path))
}
