/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import java.net.{ConnectException, SocketException}

import akka.NotUsed
import akka.stream.alpakka.solr.SolrFlowStage.{Finished, Idle, Sending}
import akka.stream.stage._
import akka.stream._
import org.apache.http.NoHttpResponseException
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.{SolrException, SolrInputDocument}

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

@deprecated(
  "you should use a specific incoming message case class: IncomingUpsertMessage/IncomingDeleteMessage/IncomingAtomicUpdateMessage"
)
object IncomingMessage {
  // Apply methods to use when not using passThrough
  def apply[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(Upsert, None, None, None, Option(source), Map.empty, NotUsed)

  def apply[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingMessage(Delete, None, Option(id), None, None, Map.empty, NotUsed)

  def apply[T](idField: String,
               idValue: String,
               routingFieldValue: String,
               updates: Map[String, Map[String, Any]]): IncomingMessage[T, NotUsed] =
    IncomingMessage(AtomicUpdate, Option(idField), Option(idValue), Some(routingFieldValue), None, updates, NotUsed)

  // Apply methods to use with passThrough
  def apply[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(Upsert, None, None, None, Option(source), Map.empty, passThrough)

  def apply[T, C](id: String, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(Delete, None, Option(id), None, None, Map.empty, passThrough)

  def apply[T, C](idField: String,
                  idValue: String,
                  routingFieldValue: String,
                  updates: Map[String, Map[String, Any]],
                  passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(AtomicUpdate,
                    Option(idField),
                    Option(idValue),
                    Option(routingFieldValue),
                    None,
                    updates,
                    passThrough)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source)

  def create[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingMessage(id)

  def create[T](idField: String,
                idValue: String,
                routingFieldValue: String,
                updates: java.util.Map[String, Map[String, Object]]): IncomingMessage[T, NotUsed] =
    IncomingMessage(idField, idValue, routingFieldValue, updates.asScala.toMap)

  // Java-api - with passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(source, passThrough)

  def create[T, C](id: String, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(id, passThrough)

  def create[T, C](idField: String,
                   idValue: String,
                   routingFieldValue: String,
                   updates: java.util.Map[String, Map[String, Object]],
                   passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(idField, idValue, routingFieldValue, updates.asScala.toMap, passThrough)

  def asScalaUpdates(jupdates: java.util.Map[String, java.util.Map[String, Object]]): Map[String, Map[String, Any]] =
    jupdates.asScala.map {
      case (k, v: java.util.Map[String, Object]) =>
        (k, v.asScala.toMap)
    }.toMap
}

object IncomingUpsertMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source)

  // Apply method to use when not using passThrough
  def apply[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(source, passThrough)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingUpsertMessage[T](source)

  // Java-api - without passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingUpsertMessage[T, C](source, passThrough)
}

object IncomingDeleteMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingMessage(id)

  def apply[T, C](id: String, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(id, passThrough)

  // Java-api - without passThrough
  def create[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingDeleteMessage[T](id)

  def create[T, C](id: String, passThrough: C): IncomingMessage[T, C] =
    IncomingDeleteMessage[T, C](id, passThrough)
}

object IncomingAtomicUpdateMessage {
  // Apply method to use when not using passThrough
  def apply[T](idField: String,
               idValue: String,
               routingFieldValue: String,
               updates: Map[String, Map[String, Any]]): IncomingMessage[T, NotUsed] =
    IncomingMessage(idField, idValue, routingFieldValue, updates)

  def apply[T, C](idField: String,
                  idValue: String,
                  routingFieldValue: String,
                  updates: Map[String, Map[String, Any]],
                  passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(idField, idValue, routingFieldValue, updates, passThrough)

  // Java-api - without passThrough
  def create[T](idField: String,
                idValue: String,
                routingFieldValue: String,
                updates: java.util.Map[String, java.util.Map[String, Object]]): IncomingMessage[T, NotUsed] =
    IncomingAtomicUpdateMessage[T](idField, idValue, routingFieldValue, IncomingMessage.asScalaUpdates(updates))

  def create[T, C](idField: String,
                   idValue: String,
                   routingFieldValue: String,
                   updates: java.util.Map[String, java.util.Map[String, Object]],
                   passThrough: C): IncomingMessage[T, C] =
    IncomingAtomicUpdateMessage[T, C](idField,
                                      idValue,
                                      routingFieldValue,
                                      IncomingMessage.asScalaUpdates(updates),
                                      passThrough)
}

final case class IncomingMessage[T, C](operation: Operation,
                                       idFieldOpt: Option[String],
                                       idFieldValueOpt: Option[String],
                                       routingFieldValueOpt: Option[String],
                                       sourceOpt: Option[T],
                                       updates: Map[String, Map[String, Any]],
                                       passThrough: C = NotUsed) {}

final case class IncomingMessageResult[T, C](idFieldOpt: Option[String],
                                             idFieldValueOpt: Option[String],
                                             routingFieldValueOpt: Option[String],
                                             sourceOpt: Option[T],
                                             updates: Map[String, Map[String, Any]],
                                             passThrough: C,
                                             status: Int)

private[solr] final class SolrFlowStage[T, C](
    collection: String,
    client: SolrClient,
    settings: SolrUpdateSettings,
    messageBinder: T => SolrInputDocument
) extends GraphStage[FlowShape[Seq[IncomingMessage[T, C]], Seq[IncomingMessageResult[T, C]]]] {

  private val in = Inlet[Seq[IncomingMessage[T, C]]]("messages")
  private val out = Outlet[Seq[IncomingMessageResult[T, C]]]("result")
  override val shape = FlowShape(in, out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes(ActorAttributes.IODispatcher)

  override def createLogic(inheritedAttributes: Attributes): SolrFlowLogic[T, C] =
    new SolrFlowLogic[T, C](collection, client, in, out, shape, settings, messageBinder)
}

sealed trait Operation
final object Upsert extends Operation
final object Delete extends Operation
final object AtomicUpdate extends Operation

private sealed trait SolrFlowState

private object SolrFlowStage {
  case object Idle extends SolrFlowState
  case object Sending extends SolrFlowState
  case object Finished extends SolrFlowState
}

private[solr] final class SolrFlowLogic[T, C](
    collection: String,
    client: SolrClient,
    in: Inlet[Seq[IncomingMessage[T, C]]],
    out: Outlet[Seq[IncomingMessageResult[T, C]]],
    shape: FlowShape[Seq[IncomingMessage[T, C]], Seq[IncomingMessageResult[T, C]]],
    settings: SolrUpdateSettings,
    messageBinder: T => SolrInputDocument
) extends TimerGraphStageLogic(shape)
    with OutHandler
    with InHandler
    with StageLogging {

  private var state: SolrFlowState = Idle
  private var failedMessages: Seq[IncomingMessage[T, C]] = Nil
  private var retryCount: Int = 0

  setHandlers(in, out, this)

  override def onPull(): Unit =
    tryPull()

  override def onPush(): Unit = {
    val messagesIn = grab(in)

    state match {
      case Idle => {
        state = Sending
        sendBulkToSolr(messagesIn)
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
    if (!isClosed(in) && !hasBeenPulled(in)) {
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
    val result = messages.map(
      m =>
        IncomingMessageResult(m.idFieldOpt,
                              m.idFieldValueOpt,
                              m.routingFieldValueOpt,
                              m.sourceOpt,
                              m.updates,
                              m.passThrough,
                              status)
    )

    emit(out, result)

    state match {
      case Finished => handleSuccess()
      case _ => state = Idle
    }
  }

  private def handleSuccess(): Unit =
    completeStage()

  private def updateBulkToSolr(messages: Seq[IncomingMessage[T, C]]): UpdateResponse = {
    val docs = messages
      .map(
        message =>
          message.sourceOpt.map { source: T =>
            messageBinder(source)
        }
      )
      .flatten
    if (log.isDebugEnabled) log.debug(s"Upsert $docs")
    client.add(collection, docs.asJava, settings.commitWithin)
  }

  private def atomicUpdateBulkToSolr(messages: Seq[IncomingMessage[T, C]]): UpdateResponse = {
    val docs = messages.map { message =>
      val doc = new SolrInputDocument()
      if (message.idFieldOpt.isEmpty || message.idFieldValueOpt.isEmpty) {
        throw new IllegalArgumentException("idfield name and idfield value should be set")
      }

      doc.addField(message.idFieldOpt.get, message.idFieldValueOpt.get)
      if (client.isInstanceOf[CloudSolrClient]) {
        if (message.routingFieldValueOpt.isEmpty)
          throw new IllegalArgumentException("routing field value should be set")
        val routerField = client.asInstanceOf[CloudSolrClient].getIdField
        if (routerField != message.idFieldOpt.get)
          doc.addField(routerField, message.routingFieldValueOpt.get)
      }
      message.updates.foreach {
        case (field, updates) => {
          val jMap = updates.asInstanceOf[Map[String, Any]].asJava
          doc.addField(field, jMap)
        }
      }
      doc
    }
    if (log.isDebugEnabled) log.debug(s"Update atomically $docs")
    client.add(collection, docs.asJava, settings.commitWithin)
  }

  private def deleteBulkToSolr(messages: Seq[IncomingMessage[T, C]]): UpdateResponse = {
    val docsIds = messages
      .filter { message =>
        message.operation == Delete && message.idFieldValueOpt.isDefined
      }
      .map { message =>
        message.idFieldValueOpt.get
      }
    if (log.isDebugEnabled) log.debug(s"Delete the ids $docsIds")
    client.deleteById(collection, docsIds.asJava, settings.commitWithin)
  }

  private def sendBulkToSolr(messages: Seq[IncomingMessage[T, C]]): Unit = {

    @tailrec
    def send(toSend: Seq[IncomingMessage[T, C]]): UpdateResponse = {
      val operation = toSend.head.operation
      //Just take a subset of this operation
      val current = toSend.takeWhile { m =>
        m.operation == operation
      }
      //send this subset
      val response = operation match {
        case Upsert => updateBulkToSolr(current)
        case AtomicUpdate => atomicUpdateBulkToSolr(current)
        case Delete => deleteBulkToSolr(current)
      }
      //Now take the remaining
      val remaining = toSend.dropWhile(m => m.operation == operation)
      if (remaining.nonEmpty) {
        send(remaining) //Important: Not really recursive, because the future breaks the recursion
      } else {
        response
      }
    }

    try {
      val response = if (messages.nonEmpty) send(messages) else new UpdateResponse
      handleResponse((messages, response.getStatus))
    } catch {
      case exception: Throwable =>
        log.error(exception, s"Unable to treat messages $messages")
        exception match {
          case NonFatal(exc) =>
            val rootCause = SolrException.getRootCause(exc)
            if (shouldRetry(rootCause)) {
              handleFailure((messages, exception))
            } else {
              val status = exc match {
                case e: SolrException => e.code()
                case _ => -1
              }
              handleResponse((messages, status))
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
