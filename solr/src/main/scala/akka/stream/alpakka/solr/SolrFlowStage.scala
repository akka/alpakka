/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import java.net.{ConnectException, SocketException}

import akka.NotUsed
import akka.stream.alpakka.solr.SolrFlowStage.{Finished, Idle, Sending}
import akka.stream.stage.{GraphStage, InHandler, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.http.NoHttpResponseException
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.{SolrException, SolrInputDocument}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal

object IncomingMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source, NotUsed)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source)

  // Java-api - with passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(source, passThrough)

}

final case class IncomingMessage[T, C](source: T, passThrough: C)

final case class IncomingMessageResult[T, C](source: T, passThrough: C, status: Int)

private[solr] final class SolrFlowStage[T, C](
    collection: String,
    client: SolrClient,
    settings: SolrUpdateSettings,
    messageBinder: T => SolrInputDocument
) extends GraphStage[FlowShape[IncomingMessage[T, C], Future[Seq[IncomingMessageResult[T, C]]]]] {

  private val in = Inlet[IncomingMessage[T, C]]("messages")
  private val out = Outlet[Future[Seq[IncomingMessageResult[T, C]]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): SolrFlowLogic[T, C] =
    new SolrFlowLogic[T, C](collection, client, in, out, shape, settings, messageBinder)
}

private sealed trait SolrFlowState

private object SolrFlowStage {
  case object Idle extends SolrFlowState
  case object Sending extends SolrFlowState
  case object Finished extends SolrFlowState
}

private[solr] final class SolrFlowLogic[T, C](
    collection: String,
    client: SolrClient,
    in: Inlet[IncomingMessage[T, C]],
    out: Outlet[Future[Seq[IncomingMessageResult[T, C]]]],
    shape: FlowShape[IncomingMessage[T, C], Future[Seq[IncomingMessageResult[T, C]]]],
    settings: SolrUpdateSettings,
    messageBinder: T => SolrInputDocument
) extends TimerGraphStageLogic(shape)
    with OutHandler
    with InHandler {

  private var state: SolrFlowState = Idle
  private val queue = new mutable.Queue[IncomingMessage[T, C]]()
  private var failedMessages: Seq[IncomingMessage[T, C]] = Nil
  private var retryCount: Int = 0

  setHandlers(in, out, this)

  override def onPull(): Unit =
    tryPull()

  override def onPush(): Unit = {
    queue.enqueue(grab(in))
    state match {
      case Idle => {
        state = Sending
        val messages = (1 to settings.bufferSize).flatMap { _ =>
          queue.dequeueFirst(_ => true)
        }
        sendBulkToSolr(messages)
      }
      case _ => ()
    }

    tryPull()
  }

  override def preStart(): Unit =
    pull(in)

  override def onTimer(timerKey: Any): Unit = {
    sendBulkToSolr(failedMessages)
    failedMessages = Nil
  }

  override def onUpstreamFailure(ex: Throwable): Unit =
    failStage(ex)

  override def onUpstreamFinish(): Unit = state match {
    case Idle => handleSuccess()
    case Sending => state = Finished
    case Finished => ()
  }

  private def tryPull(): Unit =
    if (queue.size < settings.bufferSize && !isClosed(in) && !hasBeenPulled(in)) {
      pull(in)
    }

  private def handleFailure(messages: Seq[IncomingMessage[T, C]], exc: Throwable): Unit =
    if (retryCount >= settings.maxRetry) {
      failStage(exc)
    } else {
      retryCount = retryCount + 1
      failedMessages = messages
      scheduleOnce(NotUsed, settings.retryInterval)
    }

  private def handleResponse(messages: Seq[IncomingMessage[T, C]], status: Int): Unit = {
    retryCount = 0
    val result = messages.map(m => IncomingMessageResult(m.source, m.passThrough, status))

    emit(out, Future.successful(result))

    val nextMessages = (1 to settings.bufferSize).flatMap { _ =>
      queue.dequeueFirst(_ => true)
    }

    if (nextMessages.isEmpty) {
      state match {
        case Finished => handleSuccess()
        case _ => state = Idle
      }
    } else {
      sendBulkToSolr(nextMessages)
    }
  }

  private def handleSuccess(): Unit =
    completeStage()

  private def sendBulkToSolr(messages: Seq[IncomingMessage[T, C]]): Unit =
    try {
      val docs = messages.map(message => messageBinder(message.source))
      val response = client.add(collection, docs.asJava, settings.commitWithin)
      handleResponse(messages, response.getStatus)
    } catch {
      case NonFatal(exc) =>
        val rootCause = SolrException.getRootCause(exc)
        if (shouldRetry(rootCause)) {
          handleFailure(messages, exc)
        } else {
          val status = exc match {
            case e: SolrException => e.code()
            case _ => -1
          }
          handleResponse(messages, status)
        }
    }

  private def shouldRetry(cause: Throwable): Boolean =
    cause match {
      case _: ConnectException => true
      case _: NoHttpResponseException => true
      case _: SocketException => true
      case _ => false
    }
}
