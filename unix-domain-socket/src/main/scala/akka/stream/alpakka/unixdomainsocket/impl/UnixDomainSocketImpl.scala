/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.unixdomainsocket.impl

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.nio.channels._

import akka.actor._
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.unixdomainsocket.scaladsl
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.{Done, NotUsed}
import jnr.enxio.channels.NativeSelectorProvider
import jnr.unixsocket.{UnixServerSocketChannel, UnixSocketAddress, UnixSocketChannel}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util._

/**
 * INTERNAL API
 */
@InternalApi
private[unixdomainsocket] object UnixDomainSocketImpl {

  import scaladsl.UnixDomainSocket._

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
      @volatile var receive: ReceiveContext,
      @volatile var halfClose: Boolean,
      @volatile var isOutputShutdown: Boolean,
      @volatile var isInputShutdown: Boolean
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
                  case _: SendAvailable =>
                  case ShutdownRequested if key.isValid && !sendReceiveContext.isOutputShutdown =>
                    try {
                      if (sendReceiveContext.isInputShutdown) {
                        key.cancel()
                        key.channel.close()
                      } else {
                        sendReceiveContext.isOutputShutdown = true
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE)
                        key.channel.asInstanceOf[UnixSocketChannel].shutdownOutput()
                      }
                    } catch {
                      // socket could have been closed in the meantime, so shutdownOutput will throw this
                      case _: IOException =>
                    }
                  case ShutdownRequested =>
                  case CloseRequested =>
                    key.cancel()
                    key.channel.close()
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
                      key.interestOps(key.interestOps() & ~SelectionKey.OP_READ)
                    } else {
                      queue.complete()
                      try {
                        if (!sendReceiveContext.halfClose || sendReceiveContext.isOutputShutdown) {
                          key.cancel()
                          key.channel().close()
                        } else {
                          sendReceiveContext.isInputShutdown = true
                          channel.shutdownInput()
                        }
                      } catch {
                        // socket could have been closed in the meantime, so shutdownInput will throw this
                        case _: IOException =>
                      }
                    }

                  case PendingReceiveAck(receiveQueue, receiveBuffer, pendingResult) if pendingResult.isCompleted =>
                    pendingResult.value.get match {
                      case Success(QueueOfferResult.Enqueued) =>
                        key.interestOps(key.interestOps() | SelectionKey.OP_READ)
                        sendReceiveContext.receive = ReceiveAvailable(receiveQueue, receiveBuffer)
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
        ReceiveAvailable(receiveQueue, ByteBuffer.allocate(receiveBufferSize)),
        halfClose = halfClose,
        isOutputShutdown = false,
        isInputShutdown = false
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
          case (_, done) =>
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
 * INTERNAL API
 */
@InternalApi
private[unixdomainsocket] abstract class UnixDomainSocketImpl(system: ExtendedActorSystem) extends Extension {

  import scaladsl.UnixDomainSocket._
  import UnixDomainSocketImpl._

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

  protected def bind(file: File,
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

  protected def outgoingConnection(
      remoteAddress: UnixSocketAddress,
      localAddress: Option[UnixSocketAddress] = None,
      halfClose: Boolean = true,
      connectTimeout: Duration = Duration.Inf
  ): Flow[ByteString, ByteString, Future[OutgoingConnection]] = {

    val connect: () => (Flow[ByteString, ByteString, Any], Future[Done]) = () => {
      val p = Promise[Done]
      val f = p.future

      val (context, connectionFlow) = sendReceiveStructures(sel, receiveBufferSize, sendBufferSize, halfClose)
      val ch = UnixSocketChannel.open()

      val cancellable =
        connectTimeout match {
          case d: FiniteDuration =>
            Some(system.scheduler.scheduleOnce(d, new Runnable {
              override def run(): Unit =
                ch.close()
            }))
          case _ =>
            None
        }

      val key = ch
        .configureBlocking(false)
        .register(sel, SelectionKey.OP_CONNECT, connectKey(remoteAddress, p, cancellable, context) _)

      f.failed.foreach { _ =>
        key.cancel()
        ch.close()
      }

      try { ch.connect(remoteAddress) } catch {
        case NonFatal(e) => p.tryFailure(e)
      }

      (connectionFlow, f)

    }

    val graph = new WrappedFlowStageWithMaterializedValue[ByteString, ByteString, Future[Done]](
      connect,
      Inlet("OutgoingUnixDomainSocket.in"),
      Outlet("OutgoingUnixDomainSocket.out")
    )

    Flow
      .fromGraph(graph)
      .mapMaterializedValue {
        _.map(_ => OutgoingConnection(remoteAddress, localAddress.getOrElse(new UnixSocketAddress(""))))
      }
  }
}
