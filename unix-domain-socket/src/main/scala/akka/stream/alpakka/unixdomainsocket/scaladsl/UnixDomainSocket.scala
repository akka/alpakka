/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.scaladsl

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector}

import akka.{Done, NotUsed}
import akka.actor.{
  ActorSystem,
  Cancellable,
  CoordinatedShutdown,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.util.ByteString
import jnr.enxio.channels.NativeSelectorProvider
import jnr.unixsocket.{UnixServerSocketChannel, UnixSocketAddress, UnixSocketChannel}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object UnixDomainSocket extends ExtensionId[UnixDomainSocket] with ExtensionIdProvider {

  def apply()(implicit system: ActorSystem): UnixDomainSocket = super.apply(system)

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

  private sealed abstract class ReceiveContext(
      val queue: SourceQueueWithComplete[ByteString],
      val buffer: ByteBuffer
  )
  private case class ReceiveAvailable(
      override val queue: SourceQueueWithComplete[ByteString],
      override val buffer: ByteBuffer
  ) extends ReceiveContext(queue, buffer)
  private case class PendingReceiveAck(
      override val queue: SourceQueueWithComplete[ByteString],
      override val buffer: ByteBuffer,
      pendingResult: Future[QueueOfferResult]
  ) extends ReceiveContext(queue, buffer)

  private sealed abstract class SendContext(
      val buffer: ByteBuffer
  )
  private case class SendAvailable(
      override val buffer: ByteBuffer
  ) extends SendContext(buffer)
  private case class SendRequested(
      override val buffer: ByteBuffer,
      sent: Promise[Done]
  ) extends SendContext(buffer)
  private case object CloseRequested extends SendContext(ByteString.empty.asByteBuffer)
  private case object ShutdownRequested extends SendContext(ByteString.empty.asByteBuffer)

  private class SendReceiveContext(
      @volatile var send: SendContext,
      @volatile var receive: ReceiveContext
  )

  /*
   * All NIO for UnixDomainSocket across an entire actor system is performed on just one thread. Data
   * is input/output as fast as possible with back-pressure being fully implemented e.g. if there's
   * no other thread ready to consume a receive buffer, then there is no registration for a read
   * operation.
   */
  private def nioEventLoop(sel: Selector)(implicit ec: ExecutionContext): Unit =
    while (sel.isOpen) {
      val nrOfKeysSelected = sel.select()
      if (sel.isOpen) {
        val keySelectable = nrOfKeysSelected > 0
        val keys = if (keySelectable) sel.selectedKeys().iterator() else sel.keys().iterator()
        while (keys.hasNext) {
          val key = keys.next()
          if (key != null) { // Observed as sometimes being null via sel.keys().iterator()
            if (keySelectable && (key.isAcceptable || key.isConnectable)) {
              val newConnectionOp = key.attachment().asInstanceOf[(Selector, SelectionKey) => Unit]
              newConnectionOp(sel, key)
            }
            key.attachment match {
              case null =>
              case sendReceiveContext: SendReceiveContext =>
                sendReceiveContext.send match {
                  case SendRequested(buffer, sent) if keySelectable && key.isWritable =>
                    val channel = key.channel().asInstanceOf[UnixSocketChannel]

                    val written =
                      try {
                        channel.write(buffer)
                        true
                      } catch {
                        case e: IOException =>
                          key.cancel()
                          key.channel.close()
                          sent.failure(e)
                          false
                      }

                    if (written && buffer.remaining == 0) {
                      sendReceiveContext.send = SendAvailable(buffer)
                      key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE)
                      sent.success(Done)
                    }
                  case _: SendRequested =>
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE)
                  case CloseRequested =>
                    key.cancel()
                    key.channel.close()
                  case ShutdownRequested =>
                    try {
                      key.channel().asInstanceOf[UnixSocketChannel].shutdownOutput()
                    } catch {
                      // socket could have been closed in the meantime, so shutdownOutput will throw this
                      case _: IOException =>
                    }
                  case _: SendAvailable =>
                }
                sendReceiveContext.receive match {
                  case ReceiveAvailable(queue, buffer) if keySelectable && key.isReadable =>
                    buffer.clear()

                    val channel = key.channel.asInstanceOf[UnixSocketChannel]

                    val n =
                      try {
                        channel.read(buffer)
                      } catch {
                        // socket could have been closed in the meantime, so read will throw this
                        case _: IOException => -1
                      }

                    if (n >= 0) {
                      buffer.flip()
                      val pendingResult = queue.offer(ByteString(buffer))
                      pendingResult.onComplete(_ => sel.wakeup())
                      sendReceiveContext.receive = PendingReceiveAck(queue, buffer, pendingResult)
                    } else {
                      queue.complete()
                      try {
                        channel.shutdownInput()
                      } catch {
                        // socket could have been closed in the meantime, so shutdownInput will throw this
                        case _: IOException =>
                      }
                    }

                    key.interestOps(key.interestOps() & ~SelectionKey.OP_READ)

                  case PendingReceiveAck(receiveQueue, receiveBuffer, pendingResult) if pendingResult.isCompleted =>
                    pendingResult.value.get match {
                      case Success(QueueOfferResult.Enqueued) =>
                        sendReceiveContext.receive = ReceiveAvailable(receiveQueue, receiveBuffer)
                        key.interestOps(key.interestOps() | SelectionKey.OP_READ)
                      case _ =>
                        receiveQueue.complete()
                        key.cancel()
                        key.channel.close()
                    }
                  case _: ReceiveAvailable =>
                  case _: PendingReceiveAck =>
                }
              case _: ((Selector, SelectionKey) => Unit) @unchecked =>
            }
          }
          if (keySelectable) keys.remove()
        }
      }
    }

  private def acceptKey(
      localAddress: UnixSocketAddress,
      incomingConnectionQueue: SourceQueueWithComplete[IncomingConnection],
      halfClose: Boolean,
      receiveBufferSize: Int,
      sendBufferSize: Int
  )(sel: Selector, key: SelectionKey)(implicit mat: ActorMaterializer, ec: ExecutionContext): Unit = {

    val acceptingChannel = key.channel().asInstanceOf[UnixServerSocketChannel]
    val acceptedChannel = acceptingChannel.accept()

    if (acceptedChannel != null) {
      acceptedChannel.configureBlocking(false)
      val (context, connectionFlow) = sendReceiveStructures(sel, receiveBufferSize, sendBufferSize, halfClose)
      acceptedChannel.register(sel, SelectionKey.OP_READ, context)
      incomingConnectionQueue.offer(
        IncomingConnection(localAddress, acceptingChannel.getRemoteSocketAddress, connectionFlow)
      )
    }
  }

  private def connectKey(remoteAddress: UnixSocketAddress,
                         connectionFinished: Promise[Done],
                         cancellable: Option[Cancellable],
                         sendReceiveContext: SendReceiveContext)(sel: Selector, key: SelectionKey): Unit = {

    val connectingChannel = key.channel().asInstanceOf[UnixSocketChannel]
    cancellable.foreach(_.cancel())
    try {
      connectingChannel.register(sel, SelectionKey.OP_READ, sendReceiveContext)
      val finishExpected = connectingChannel.finishConnect()
      require(finishExpected, "Internal error - our call to connection finish wasn't expected.")
      connectionFinished.trySuccess(Done)
    } catch {
      case NonFatal(e) =>
        connectionFinished.tryFailure(e)
        key.cancel()
    }
  }

  private def sendReceiveStructures(sel: Selector, receiveBufferSize: Int, sendBufferSize: Int, halfClose: Boolean)(
      implicit mat: ActorMaterializer,
      ec: ExecutionContext
  ): (SendReceiveContext, Flow[ByteString, ByteString, NotUsed]) = {

    val (receiveQueue, receiveSource) =
      Source
        .queue[ByteString](2, OverflowStrategy.backpressure)
        .prefixAndTail(0)
        .map(_._2)
        .toMat(Sink.head)(Keep.both)
        .run()
    val sendReceiveContext =
      new SendReceiveContext(
        SendAvailable(ByteBuffer.allocate(sendBufferSize)),
        ReceiveAvailable(receiveQueue, ByteBuffer.allocate(receiveBufferSize))
      ) // FIXME: No need for the costly allocation of direct buffers yet given https://github.com/jnr/jnr-unixsocket/pull/49

    val sendSink = Sink.fromGraph(
      Flow[ByteString]
        .expand { bytes =>
          if (bytes.size <= sendBufferSize) {
            Iterator.single(bytes)
          } else {
            @annotation.tailrec
            def splitToBufferSize(bytes: ByteString, acc: Vector[ByteString]): Vector[ByteString] =
              if (bytes.nonEmpty) {
                val (left, right) = bytes.splitAt(sendBufferSize)
                splitToBufferSize(right, acc :+ left)
              } else {
                acc
              }
            splitToBufferSize(bytes, Vector.empty).toIterator
          }
        }
        .mapAsync(1) { bytes =>
          // Note - it is an error to get here and not have an AvailableSendContext
          val sent = Promise[Done]
          val sendBuffer = sendReceiveContext.send.buffer
          sendBuffer.clear()
          val copied = bytes.copyToBuffer(sendBuffer)
          sendBuffer.flip()
          require(copied == bytes.size) // It is an error to exceed our buffer size given the above expand
          sendReceiveContext.send = SendRequested(sendBuffer, sent)
          sel.wakeup()
          sent.future.map(_ => bytes)
        }
        .watchTermination() {
          case (m, done) =>
            done.onComplete { _ =>
              sendReceiveContext.send = if (halfClose) {
                ShutdownRequested
              } else {
                receiveQueue.complete()
                CloseRequested
              }
              sel.wakeup()
            }
            Keep.left
        }
        .to(Sink.ignore)
    )

    (sendReceiveContext, Flow.fromSinkAndSource(sendSink, Source.fromFutureSource(receiveSource)))
  }
}

/**
 * Provides Unix Domain Socket functionality to Akka Streams with an interface similar to Akka's Tcp class.
 */
final class UnixDomainSocket(system: ExtendedActorSystem) extends Extension {

  import UnixDomainSocket._

  private implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
  import system.dispatcher

  private val sel = NativeSelectorProvider.getInstance.openSelector

  private val ioThread = new Thread(new Runnable {
    override def run(): Unit =
      nioEventLoop(sel)
  }, "unix-domain-socket-io")
  ioThread.start()

  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseServiceStop, "stopUnixDomainSocket") { () =>
    sel.close() // Not much else that we can do
    Future.successful(Done)
  }

  private val receiveBufferSize: Int =
    system.settings.config.getBytes("akka.stream.alpakka.unix-domain-socket.receive-buffer-size").toInt
  private val sendBufferSize: Int =
    system.settings.config.getBytes("akka.stream.alpakka.unix-domain-socket.send-buffer-size").toInt

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
   * @param file      The file to listen on
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
  def bind(file: File,
           backlog: Int = 128,
           halfClose: Boolean = false): Source[IncomingConnection, Future[ServerBinding]] = {

    val (incomingConnectionQueue, incomingConnectionSource) =
      Source
        .queue[IncomingConnection](2, OverflowStrategy.backpressure)
        .prefixAndTail(0)
        .map {
          case (_, source) =>
            source
              .watchTermination() { (mat, done) =>
                done
                  .andThen {
                    case _ =>
                      try {
                        file.delete()
                      } catch {
                        case NonFatal(_) =>
                      }
                  }
                mat
              }
        }
        .toMat(Sink.head)(Keep.both)
        .run()

    val serverBinding = Promise[ServerBinding]

    val channel = UnixServerSocketChannel.open()
    channel.configureBlocking(false)
    val address = new UnixSocketAddress(file)
    val registeredKey =
      channel.register(sel,
                       SelectionKey.OP_ACCEPT,
                       acceptKey(address, incomingConnectionQueue, halfClose, receiveBufferSize, sendBufferSize) _)
    try {
      channel.socket().bind(address, backlog)
      sel.wakeup()
      serverBinding.success(
        ServerBinding(address) { () =>
          registeredKey.cancel()
          channel.close()
          incomingConnectionQueue.complete()
          incomingConnectionQueue.watchCompletion().map(_ => ())
        }
      )
    } catch {
      case NonFatal(e) =>
        registeredKey.cancel()
        channel.close()
        incomingConnectionQueue.fail(e)
        serverBinding.failure(e)
    }

    Source
      .fromFutureSource(incomingConnectionSource)
      .mapMaterializedValue(_ => serverBinding.future)
  }

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
   * @param file      The file to listen on
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
                    file: File,
                    backlog: Int = 128,
                    halfClose: Boolean = false): Future[ServerBinding] =
    bind(file, backlog, halfClose)
      .to(Sink.foreach { conn: IncomingConnection ⇒
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
  def outgoingConnection(
      remoteAddress: UnixSocketAddress,
      localAddress: Option[UnixSocketAddress] = None,
      halfClose: Boolean = true,
      connectTimeout: Duration = Duration.Inf
  ): Flow[ByteString, ByteString, Future[OutgoingConnection]] = {

    val channel = UnixSocketChannel.open()
    channel.configureBlocking(false)
    val connectionFinished = Promise[Done]
    val cancellable =
      connectTimeout match {
        case d: FiniteDuration =>
          Some(system.scheduler.scheduleOnce(d, new Runnable {
            override def run(): Unit =
              channel.close()
          }))
        case _ =>
          None
      }
    val (context, connectionFlow) = sendReceiveStructures(sel, receiveBufferSize, sendBufferSize, halfClose)
    val registeredKey =
      channel
        .register(sel, SelectionKey.OP_CONNECT, connectKey(remoteAddress, connectionFinished, cancellable, context) _)
    val connection = Try(channel.connect(remoteAddress))
    connection.failed.foreach(e => connectionFinished.failure(e))

    connectionFlow
      .merge(Source.fromFuture(connectionFinished.future.map(_ => ByteString.empty)))
      .filter(_.nonEmpty) // We merge above so that we can get connection failures - we're not interested in the empty bytes though
      .mapMaterializedValue { _ =>
        connection match {
          case Success(_) =>
            connectionFinished.future
              .map(_ => OutgoingConnection(remoteAddress, localAddress.getOrElse(new UnixSocketAddress(""))))
          case Failure(e) =>
            registeredKey.cancel()
            channel.close()
            Future.failed(e)
        }
      }
  }

  /**
   * Creates an [[UnixDomainSocket.OutgoingConnection]] without specifying options.
   * It represents a prospective Unix Domain client connection to the given endpoint.
   *
   * Note that the ByteString chunk boundaries are not retained across the network,
   * to achieve application level chunks you have to introduce explicit framing in your streams,
   * for example using the [[akka.stream.scaladsl.Framing]] stages.
   */
  def outgoingConnection(file: File): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    outgoingConnection(new UnixSocketAddress(file))
}
