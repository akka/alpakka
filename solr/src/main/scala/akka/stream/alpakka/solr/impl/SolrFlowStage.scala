/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.solr.impl

import akka.annotation.InternalApi
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.alpakka.solr._
import akka.stream.stage._
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.SolrInputDocument

import scala.annotation.tailrec
import scala.util.control.NonFatal

import scala.collection.immutable
import scala.collection.JavaConverters._

/**
 * Internal API
 *
 * @tparam T
 * @tparam C pass-through type
 */
@InternalApi
private[solr] final class SolrFlowStage[T, C](
    collection: String,
    client: SolrClient,
    settings: SolrUpdateSettings,
    messageBinder: T => SolrInputDocument
) extends GraphStage[FlowShape[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]]]] {

  private val in = Inlet[immutable.Seq[WriteMessage[T, C]]]("messages")
  private val out = Outlet[immutable.Seq[WriteResult[T, C]]]("result")
  override val shape = FlowShape(in, out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes(ActorAttributes.IODispatcher)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    def decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
    new SolrFlowLogic[T, C](decider, collection, client, in, out, shape, settings, messageBinder)
  }
}

private final class SolrFlowLogic[T, C](
    decider: Supervision.Decider,
    collection: String,
    client: SolrClient,
    in: Inlet[immutable.Seq[WriteMessage[T, C]]],
    out: Outlet[immutable.Seq[WriteResult[T, C]]],
    shape: FlowShape[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]]],
    settings: SolrUpdateSettings,
    messageBinder: T => SolrInputDocument
) extends GraphStageLogic(shape)
    with OutHandler
    with InHandler
    with StageLogging {

  setHandlers(in, out, this)

  override def onPull(): Unit =
    tryPull()

  override def onPush(): Unit = {
    val messagesIn = grab(in)
    try {
      sendBulkToSolr(messagesIn)
      tryPull()
    } catch {
      case NonFatal(ex) ⇒
        decider(ex) match {
          case Supervision.Stop ⇒ failStage(ex)
          case _ ⇒ tryPull() // for resume and restart strategies tryPull
        }
    }
  }

  private def tryPull(): Unit =
    if (!isClosed(in) && !hasBeenPulled(in)) {
      pull(in)
    }

  private def updateBulkToSolr(messages: immutable.Seq[WriteMessage[T, C]]): UpdateResponse = {
    val docs = messages.flatMap(_.source.map(messageBinder))

    if (log.isDebugEnabled) log.debug("Upsert {}", docs)
    client.add(collection, docs.asJava, settings.commitWithin)
  }

  private def atomicUpdateBulkToSolr(messages: immutable.Seq[WriteMessage[T, C]]): UpdateResponse = {
    val docs = messages.map { message =>
      val doc = new SolrInputDocument()

      message.idField.foreach { idField =>
        message.idFieldValue.foreach { idFieldValue =>
          doc.addField(idField, idFieldValue)
        }
      }

      message.routingFieldValue.foreach { routingFieldValue =>
        val routingField = client match {
          case csc: CloudSolrClient =>
            Option(csc.getIdField)
          case _ => None
        }
        routingField.foreach { routingField =>
          message.idField.foreach { idField =>
            if (routingField != idField)
              doc.addField(routingField, routingFieldValue)
          }
        }
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

  private def deleteBulkToSolrByIds(messages: immutable.Seq[WriteMessage[T, C]]): UpdateResponse = {
    val docsIds = messages
      .filter { message =>
        message.operation == DeleteByIds && message.idFieldValue.isDefined
      }
      .map { message =>
        message.idFieldValue.get
      }
    if (log.isDebugEnabled) log.debug(s"Delete the ids $docsIds")
    client.deleteById(collection, docsIds.asJava, settings.commitWithin)
  }

  private def deleteEachByQuery(messages: immutable.Seq[WriteMessage[T, C]]): UpdateResponse = {
    val responses = messages.map { message =>
      val query = message.query.get
      if (log.isDebugEnabled) log.debug(s"Delete by the query $query")
      val req = new UpdateRequest()
      if (message.routingFieldValue.isDefined) req.setParam("_route_", message.routingFieldValue.get)
      req.deleteByQuery(query)
      req.setCommitWithin(settings.commitWithin)
      req.process(client, collection)
    }
    responses.find(_.getStatus != 0).getOrElse(responses.head)
  }

  private def sendBulkToSolr(messages: immutable.Seq[WriteMessage[T, C]]): Unit = {

    @tailrec
    def send(toSend: immutable.Seq[WriteMessage[T, C]]): Option[UpdateResponse] = {
      val operation = toSend.head.operation
      //Just take a subset of this operation
      val (current, remaining) = toSend.span { m =>
        m.operation == operation
      }
      //send this subset
      val response = operation match {
        case Upsert => Option(updateBulkToSolr(current))
        case AtomicUpdate => Option(atomicUpdateBulkToSolr(current))
        case DeleteByIds => Option(deleteBulkToSolrByIds(current))
        case DeleteByQuery => Option(deleteEachByQuery(current))
        case PassThrough => None
      }
      if (remaining.nonEmpty) {
        send(remaining)
      } else {
        response
      }
    }

    val response = if (messages.nonEmpty) send(messages).fold(0) { _.getStatus } else 0

    log.debug("Handle the response with {}", response)
    val results = messages.map(
      m =>
        WriteResult(m.idField,
                    m.idFieldValue,
                    m.routingFieldValue,
                    m.query,
                    m.source,
                    m.updates,
                    m.passThrough,
                    response)
    )
    emit(out, results)
  }
}
