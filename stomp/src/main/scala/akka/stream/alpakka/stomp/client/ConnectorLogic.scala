/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.stage.GraphStageLogic
import io.vertx.core.AsyncResult
import io.vertx.ext.stomp.{StompClientConnection, Frame => VertxFrame}

import scala.concurrent.Promise

/**
 * Shared logic for Source and Sink
 */
private[client] trait ConnectorLogic {
  this: GraphStageLogic =>

  val closeCallback = getAsyncCallback[StompClientConnection](_ => {
    promise.trySuccess(Done)
    completeStage()
  })
  val errorCallback = getAsyncCallback[VertxFrame](frame => {
    acknowledge(frame)
    val ex = StompProtocolError(frame)
    failCallback.invoke(ex)
  })
  val dropCallback = getAsyncCallback[StompClientConnection]((dropped: StompClientConnection) => {
    val ex = StompClientConnectionDropped(dropped)
    failCallback.invoke(ex)
  })
  val failCallback = getAsyncCallback[Throwable](ex => {
    promise.tryFailure(ex)
    failStage(ex)
  })
  val fullFillOnConnection = false
  var connection: StompClientConnection = _

  def settings: ConnectorSettings

  def promise: Promise[Done]

  def whenConnected(): Unit

  def onFailure(ex: Throwable): Unit

  override def preStart(): Unit = {

    val connectCallback = getAsyncCallback[StompClientConnection](conn => {
      connection = conn
      addHandlers()
      whenConnected()
      if (fullFillOnConnection) promise.trySuccess(Done)
    })

    // connecting async
    settings.connectionProvider.getStompClient
      .connect({
        ar: AsyncResult[StompClientConnection] => {
          if (ar.succeeded()) {
            connectCallback.invoke(ar.result())
          } else {
            if (fullFillOnConnection) promise.tryFailure(ar.cause)
            throw ar.cause()
          }
        }
      }
      )
  }

  def acknowledge(frame: VertxFrame): Unit =
    if (settings.withAck && frame.getHeaders.containsKey(VertxFrame.ACK)) {
      connection.ack(frame.getAck)
    }

  def addHandlers(): Unit = {
    failHandler(connection)
    closeHandler(connection)
    errorHandler(connection)
    writeHandler(connection)
    dropHandler(connection)
    receiveHandler(connection)
    writeHandler(connection)
  }

  def writeHandler(connection: StompClientConnection): Unit = ()

  /**
   * When a message is received by the connection
   */
  def receiveHandler(connection: StompClientConnection): Unit

  def dropHandler(connection: StompClientConnection): Unit =
    connection.connectionDroppedHandler({
      dropCallback.invoke(_)
    })

  /**
   * An ERROR frame is received
   */
  def errorHandler(connection: StompClientConnection): Unit =
    connection.errorHandler(errorCallback.invoke(_))

  /**
   * When connection get closed
   */
  def closeHandler(connection: StompClientConnection): Unit =
    connection.closeHandler(closeCallback.invoke(_))

  /**
   * Upon TCP-errors
   */
  private def failHandler(connection: StompClientConnection): Unit =
    connection.exceptionHandler(failCallback.invoke(_))

  /** remember to call if overriding! */
  override def postStop(): Unit =
    if (connection != null && connection.isConnected) connection.disconnect()

}
