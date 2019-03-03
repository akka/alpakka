/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.javadsl

import java.io.File
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.compat.java8.OptionConverters._
import scala.compat.java8.FutureConverters._
import akka.NotUsed
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.alpakka.unixdomainsocket.scaladsl.{UnixDomainSocket => ScalaUnixDomainSocket}
import akka.stream.javadsl.{Flow, Source}
import akka.stream.Materializer
import akka.util.ByteString
import jnr.unixsocket.UnixSocketAddress

import scala.concurrent.duration.Duration

object UnixDomainSocket extends ExtensionId[UnixDomainSocket] with ExtensionIdProvider {

  /**
   * Represents a prospective UnixDomainSocket server binding.
   */
  final class ServerBinding private[akka] (delegate: ScalaUnixDomainSocket.ServerBinding) {

    /**
     * The local address of the endpoint bound by the materialization of the `connections` [[akka.stream.javadsl.Source Source]].
     */
    def localAddress: UnixSocketAddress = delegate.localAddress

    /**
     * Asynchronously triggers the unbinding of the port that was bound by the materialization of the `connections`
     * [[akka.stream.javadsl.Source Source]].
     *
     * The produced [[java.util.concurrent.CompletionStage]] is fulfilled when the unbinding has been completed.
     */
    def unbind(): CompletionStage[Unit] = delegate.unbind().toJava
  }

  /**
   * Represents an accepted incoming UnixDomainSocket connection.
   */
  final class IncomingConnection private[akka] (delegate: ScalaUnixDomainSocket.IncomingConnection) {

    /**
     * The local address this connection is bound to.
     */
    def localAddress: UnixSocketAddress = delegate.localAddress

    /**
     * The remote address this connection is bound to.
     */
    def remoteAddress: UnixSocketAddress = delegate.remoteAddress

    /**
     * Handles the connection using the given flow, which is materialized exactly once and the respective
     * materialized value is returned.
     *
     * Convenience shortcut for: `flow.join(handler).run()`.
     */
    def handleWith[Mat](handler: Flow[ByteString, ByteString, Mat], materializer: Materializer): Mat =
      delegate.handleWith(handler.asScala)(materializer)

    /**
     * A flow representing the client on the other side of the connection.
     * This flow can be materialized only once.
     */
    def flow: Flow[ByteString, ByteString, NotUsed] = new Flow(delegate.flow)
  }

  /**
   * Represents a prospective outgoing UnixDomainSocket connection.
   */
  final class OutgoingConnection private[akka] (delegate: ScalaUnixDomainSocket.OutgoingConnection) {

    /**
     * The remote address this connection is or will be bound to.
     */
    def remoteAddress: UnixSocketAddress = delegate.remoteAddress

    /**
     * The local address of the endpoint bound by the materialization of the connection materialization.
     */
    def localAddress: UnixSocketAddress = delegate.localAddress
  }

  override def get(system: ActorSystem): UnixDomainSocket =
    super.get(system)

  def lookup(): ExtensionId[_ <: Extension] =
    UnixDomainSocket

  def createExtension(system: ExtendedActorSystem): UnixDomainSocket =
    new UnixDomainSocket(system)
}

final class UnixDomainSocket(system: ExtendedActorSystem) extends akka.actor.Extension {
  import UnixDomainSocket._
  import akka.dispatch.ExecutionContexts.{sameThreadExecutionContext ⇒ ec}

  private lazy val delegate: ScalaUnixDomainSocket = ScalaUnixDomainSocket(system)

  /**
   * Creates a [[UnixDomainSocket.ServerBinding]] instance which represents a prospective UnixDomainSocket server binding on the given `endpoint`.
   *
   * Please note that the startup of the server is asynchronous, i.e. after materializing the enclosing
   * [[akka.stream.scaladsl.RunnableGraph]] the server is not immediately available. Only after the materialized future
   * completes is the server ready to accept client connections.
   *
   * TODO: Support idleTimeout as per Tcp.
   *
   * @param file      The file to listen on
   * @param backlog   Controls the size of the connection backlog
   * @param halfClose
   *                  Controls whether the connection is kept open even after writing has been completed to the accepted
   *                  UnixDomainSocket connections.
   *                  If set to true, the connection will implement the UnixDomainSocket half-close mechanism, allowing the client to
   *                  write to the connection even after the server has finished writing. The UnixDomainSocket socket is only closed
   *                  after both the client and server finished writing.
   *                  If set to false, the connection will immediately closed once the server closes its write side,
   *                  independently whether the client is still attempting to write. This setting is recommended
   *                  for servers, and therefore it is the default setting.
   */
  def bind(file: File, backlog: Int, halfClose: Boolean): Source[IncomingConnection, CompletionStage[ServerBinding]] =
    Source.fromGraph(
      delegate
        .bind(file, backlog, halfClose)
        .map(new IncomingConnection(_))
        .mapMaterializedValue(_.map(new ServerBinding(_))(ec).toJava)
    )

  /**
   * Creates a [[UnixDomainSocket.ServerBinding]] without specifying options.
   * It represents a prospective UnixDomainSocket server binding on the given `endpoint`.
   *
   * Please note that the startup of the server is asynchronous, i.e. after materializing the enclosing
   * [[akka.stream.scaladsl.RunnableGraph]] the server is not immediately available. Only after the materialized future
   * completes is the server ready to accept client connections.
   */
  def bind(file: File): Source[IncomingConnection, CompletionStage[ServerBinding]] =
    Source.fromGraph(
      delegate
        .bind(file)
        .map(new IncomingConnection(_))
        .mapMaterializedValue(_.map(new ServerBinding(_))(ec).toJava)
    )

  /**
   * Creates an [[UnixDomainSocket.OutgoingConnection]] instance representing a prospective UnixDomainSocket client connection to the given endpoint.
   *
   * Note that the ByteString chunk boundaries are not retained across the network,
   * to achieve application level chunks you have to introduce explicit framing in your streams,
   * for example using the [[akka.stream.javadsl.Framing]] stages.
   *
   * TODO: Support idleTimeout as per Tcp.
   *
   * @param remoteAddress The remote address to connect to
   * @param localAddress  Optional local address for the connection
   * @param halfClose
   *                  Controls whether the connection is kept open even after writing has been completed to the accepted
   *                  UnixDomainSocket connections.
   *                  If set to true, the connection will implement the UnixDomainSocket half-close mechanism, allowing the server to
   *                  write to the connection even after the client has finished writing. The UnixDomainSocket socket is only closed
   *                  after both the client and server finished writing. This setting is recommended for clients and
   *                  therefore it is the default setting.
   *                  If set to false, the connection will immediately closed once the client closes its write side,
   *                  independently whether the server is still attempting to write.
   */
  def outgoingConnection(remoteAddress: UnixSocketAddress,
                         localAddress: Optional[UnixSocketAddress],
                         halfClose: Boolean,
                         connectTimeout: Duration): Flow[ByteString, ByteString, CompletionStage[OutgoingConnection]] =
    Flow.fromGraph(
      delegate
        .outgoingConnection(remoteAddress, localAddress.asScala, halfClose, connectTimeout)
        .mapMaterializedValue(_.map(new OutgoingConnection(_))(ec).toJava)
    )

  /**
   * Creates an [[UnixDomainSocket.OutgoingConnection]] without specifying options.
   * It represents a prospective UnixDomainSocket client connection to the given endpoint.
   *
   * TODO: Support idleTimeout as per Tcp.
   *
   * Note that the ByteString chunk boundaries are not retained across the network,
   * to achieve application level chunks you have to introduce explicit framing in your streams,
   * for example using the [[akka.stream.javadsl.Framing]] stages.
   */
  def outgoingConnection(file: File): Flow[ByteString, ByteString, CompletionStage[OutgoingConnection]] =
    Flow.fromGraph(
      delegate
        .outgoingConnection(new UnixSocketAddress(file))
        .mapMaterializedValue(_.map(new OutgoingConnection(_))(ec).toJava)
    )

}
