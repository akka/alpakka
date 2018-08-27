/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import java.net.{ConnectException, SocketException}

import akka.NotUsed

import scala.concurrent.{blocking, ExecutionContext, Future}
import akka.stream.alpakka.solr.SolrFlowStage.{Finished, Idle, Sending}
import akka.stream.stage._
import akka.stream._
import org.apache.http.NoHttpResponseException
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.{SolrException, SolrInputDocument}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

object IncomingMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(Update, None, Some(source), NotUsed)

  def apply[T](operation: Operation, source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(operation, None, Some(source), NotUsed)

  def apply(id: String): IncomingMessage[NotUsed, NotUsed] =
    IncomingMessage(Delete, Some(id), None, NotUsed)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source)

  // Java-api - with passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(Update, None, Some(source), passThrough)

  def create(id: String): IncomingMessage[NotUsed, NotUsed] =
    IncomingMessage(id)

  // Java-api - with passThrough
  def create[T, C](operation: Operation,
                   idOpt: Option[String],
                   sourceOpt: Option[T],
                   passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(operation, None, sourceOpt, passThrough)

}

final case class IncomingMessage[T, C](operation: Operation,
                                       idOpt: Option[String],
                                       sourceOpt: Option[T],
                                       passThrough: C = NotUsed)

final case class IncomingMessageResult[T, C](id: Option[String], sourceOpt: Option[T], passThrough: C, status: Int)

private[solr] final class SolrFlowStage[T, C](
    collection: String,
    client: SolrClient,
    settings: SolrUpdateSettings,
    messageBinder: Option[T => SolrInputDocument]
) extends GraphStage[FlowShape[IncomingMessage[T, C], Future[Seq[IncomingMessageResult[T, C]]]]] {

  private val in = Inlet[IncomingMessage[T, C]]("messages")
  private val out = Outlet[Future[Seq[IncomingMessageResult[T, C]]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): SolrFlowLogic[T, C] =
    new SolrFlowLogic[T, C](collection, client, in, out, shape, settings, messageBinder)
}

sealed trait Operation
final object Update extends Operation
final object Delete extends Operation

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
    messageBinder: Option[T => SolrInputDocument]
) extends TimerGraphStageLogic(shape)
    with OutHandler
    with InHandler
    with StageLogging {

  private var state: SolrFlowState = Idle
  private val queue = new mutable.Queue[IncomingMessage[T, C]]()
  private var failedMessages: Seq[IncomingMessage[T, C]] = Nil
  private var retryCount: Int = 0
  private val failureHandler = getAsyncCallback[(Seq[IncomingMessage[T, C]], Throwable)](handleFailure)
  private val responseHandler = getAsyncCallback[(Seq[IncomingMessage[T, C]], Int)](handleResponse)

  implicit private var dispatcher: ExecutionContext = _

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

  override def preStart(): Unit = {
    dispatcher = materializer.asInstanceOf[ActorMaterializer].system.dispatchers.lookup(getDispatcher.dispatcher)
    pull(in)
  }

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

  private def handleFailure(args: (Seq[IncomingMessage[T, C]], Throwable)): Unit = {
    val (messages, exc) = args
    if (retryCount >= settings.maxRetry) {
      log.warning(s"Received error from solr. Giving up after $retryCount tries. Error: ${exc.toString}")
      failStage(exc)
    } else {
      retryCount = retryCount + 1
      log.warning(
        s"Received error from solr. (re)tryCount: $retryCount maxTries: ${settings.maxRetry}. Error: ${exc.toString}"
      )
      failedMessages = messages
      scheduleOnce(NotUsed, settings.retryInterval)
    }
  }

  private def handleResponse(args: (Seq[IncomingMessage[T, C]], Int)): Unit = {
    val (messages, status) = args
    log.debug(s"Handle the response with $status")
    retryCount = 0
    val result = messages.map(m => IncomingMessageResult(m.idOpt, m.sourceOpt, m.passThrough, status))

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
      if (log.isDebugEnabled) {
        log.debug(s"Remains ${nextMessages.size} to send")
      }
      sendBulkToSolr(nextMessages)
    }
  }

  private def handleSuccess(): Unit =
    completeStage()

  private def fDeleteBulkToSolr(messages: Seq[IncomingMessage[T, C]]): Future[UpdateResponse] = {
    val docsIds = messages
      .filter { message =>
        message.operation == Delete && message.idOpt.isDefined
      }
      .map { message =>
        message.idOpt.get
      }
    if (log.isDebugEnabled) log.debug(s"Delete the ids $docsIds")
    Future {
      blocking {
        client.deleteById(collection, docsIds.asJava, settings.commitWithin)
      }
    }
  }

  private[solr] def getDispatcher =
    attributes.get[ActorAttributes.Dispatcher](
      ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
    ) match {
      case ActorAttributes.Dispatcher("") =>
        ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
      case d => d
    }

  private def fUpdateBulkToSolr(messages: Seq[IncomingMessage[T, C]]): Future[UpdateResponse] = {
    val docs = messages.map(message => messageBinder.get(message.sourceOpt.get))
    Future {
      blocking {
        client.add(collection, docs.asJava, settings.commitWithin)
      }
    }
  }

  private def sendBulkToSolr(messages: Seq[IncomingMessage[T, C]]): Unit = {
    val (updates, deletes) = messages.partition { m =>
      m.operation == Update
    }

    if (updates.isEmpty && deletes.isEmpty) {
      //Nothing to do
    } else {
      val fBulkResult = if (updates.isEmpty) {
        fDeleteBulkToSolr(deletes)
      } else if (deletes.isEmpty) {
        fUpdateBulkToSolr(updates)
      } else {
        fUpdateBulkToSolr(updates).flatMap(_ => fDeleteBulkToSolr(deletes))
      }
      fBulkResult.onComplete {
        case Success(response) => responseHandler.invoke((messages, response.getStatus))
        case Failure(exception) =>
          log.error(exception, "Unable to send messages to SolR")
          exception match {
            case NonFatal(exc) =>
              val rootCause = SolrException.getRootCause(exc)
              if (shouldRetry(rootCause)) {
                failureHandler.invoke((messages, exception))
              } else {
                val status = exc match {
                  case e: SolrException => e.code()
                  case _ => -1
                }
                responseHandler.invoke((messages, status))
              }
          }
      }
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
