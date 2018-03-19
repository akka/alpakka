/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import io.vertx.core.Vertx
import io.vertx.ext.stomp.{
  Destination,
  Frame,
  StompServer,
  StompServerConnection,
  StompServerHandler,
  StompServerOptions
}
import io.vertx.ext.stomp.impl.Topic
import io.vertx.ext.stomp.impl.Topic.Subscription

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

private[stomp] object Server {

//  val vertx: Vertx = Vertx.vertx(new VertxOptions().setBlockedThreadCheckInterval(10))
  val vertx = Vertx.vertx()
  val patienceWithServer: FiniteDuration = 1.seconds

  def getStompServer(handler: Option[StompServerHandler] = None, port: Int = 61613): StompServer =
    Await.result(stompServerFuture(handler, port), patienceWithServer)

  private def stompServerFuture(handler: Option[StompServerHandler] = None, port: Int = 61613) = {
    val serverHandler = handler.getOrElse(StompServerHandler.create(vertx))
    val promise = Promise[StompServer]()
    StompServer
      .create(vertx, new StompServerOptions().setPort(port))
      .handler(serverHandler)
      .listen(
        ar =>
          if (ar.succeeded()) {
            promise success ar.result()
          } else {
            promise failure ar.cause()
        }
      )
    promise.future
  }

  def closeAwaitStompServer(server: StompServer): Future[Done] = {
    val promise = Promise[Done]()
    server.close(
      ar =>
        if (ar.succeeded()) promise.success(Done)
        else promise.failure(ar.cause())
    )
    Await.ready(promise.future, patienceWithServer)
  }

  def accumulateHandler(accumulator: Frame => Unit): StompServerHandler =
    StompServerHandler
      .create(vertx)
      .receivedFrameHandler(ar => {
        // accumulate SEND received by Server
        if (ar.frame().getCommand() == Frame.Command.SEND) {
          accumulator(ar.frame())
        }
      })

  def stompServerWithTopicAndQueue(port: Int): StompServer =
    getStompServer(
      Some(
        StompServerHandler
          .create(vertx)
          .destinationFactory((v, name) => {
            new StompServerTopic(v, name)
          })
      ),
      port
    )

  /**
   * Describe a topic in the stomp server, that is able to `feel` the ack.
   *
   * Current vertx-stomp-server with vertx-stomp-server-topic does not handle the `ack`. We extend the default Topic implementation in order to handle a Queues of messages and ack of them.
   *
   */
  class StompServerTopic(vertx: Vertx, destination: String) extends Topic(vertx, destination) {
    import collection.mutable.{Queue => mQueue}
    case class Subs(stompServerConnection: StompServerConnection,
                    var waitingAck: Option[String] = None,
                    var pendingFrames: mQueue[Frame] = mQueue())

    var internalSubs: Map[Subscription, Subs] = Map()

    override def subscribe(connection: StompServerConnection, frame: Frame): Destination = {
      super.subscribe(connection, frame)
      // hack to be able to access the connection
      val i = subscriptions.size()
      val last = subscriptions.get(i - 1)
      internalSubs = internalSubs + (last -> Subs(connection))
      this
    }

    override def ack(connection: StompServerConnection, frame: Frame): Boolean = {
      val ack = frame.getId

      internalSubs
        .find(_._2.stompServerConnection == connection)
        .find(_._2.waitingAck.contains(ack))
        .map {
          case (subscription: Subscription, subs: Subs) =>
            subs.waitingAck = None
            if (subs.pendingFrames.nonEmpty) deliver(subs, subscription, subs.pendingFrames.dequeue())
        }
        .nonEmpty
    }

    def deliver(subs: Subs, subscription: Subscription, frame: Frame): StompServerConnection = {
      import collection.JavaConverters._

      val messageId = java.util.UUID.randomUUID().toString
      val message = Topic.transform(frame, subscription, messageId)
      if (message.getHeaders.asScala.contains(Frame.ACK)) {
        subs.waitingAck = Some(message.getAck)
      }
      subs.stompServerConnection.write(message)
    }
    def enqueue(subs: Subs, frame: Frame): Unit =
      subs.pendingFrames.enqueue(frame)

    override def dispatch(connection: StompServerConnection, frame: Frame): Destination = {
      import collection.JavaConverters._
      val coll = for {
        subscription <- subscriptions.asScala
        sub: Subs <- internalSubs.get(subscription)
      } yield (sub, subscription)
      coll.foreach {
        case (subs: Subs, subscription: Subscription) if subs.waitingAck.isEmpty =>
          deliver(subs, subscription, frame)
        case (subs, _) => enqueue(subs, frame)
      }
      this
    }
  }
}
