/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.orientdb.{OrientDBUpdateSettings, OrientDbWriteMessage}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.tx.OTransaction

import scala.collection.mutable
import scala.collection.immutable

/**
 * INTERNAL API
 */
@InternalApi
private[orientdb] class OrientDBFlowStage[T, C](
    className: String,
    settings: OrientDBUpdateSettings,
    clazz: Option[Class[T]]
) extends GraphStage[FlowShape[immutable.Seq[OrientDbWriteMessage[T, C]], immutable.Seq[OrientDbWriteMessage[T, C]]]] {

  private val in = Inlet[immutable.Seq[OrientDbWriteMessage[T, C]]]("in")
  private val out = Outlet[immutable.Seq[OrientDbWriteMessage[T, C]]]("out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private var failedMessages: immutable.Seq[OrientDbWriteMessage[T, C]] = Nil
      private var retryCount: Int = 0

      private var client: ODatabaseDocumentTx = _
      private var oObjectClient: OObjectDatabaseTx = _

      override def preStart(): Unit = {
        client = settings.oDatabasePool.acquire()
        oObjectClient = new OObjectDatabaseTx(client)
        pull(in)
      }

      override def postStop(): Unit = {
        oObjectClient.close()
        client.close()
      }

      override def onTimer(timerKey: Any): Unit = {
        sendOSQLBulkInsertRequest(failedMessages)
        failedMessages = Nil
      }

      private def handleFailure(args: (immutable.Seq[OrientDbWriteMessage[T, C]], Throwable)): Unit = {
        val (messages, exception) = args
        if (retryCount >= settings.maxRetry) {
          failStage(exception)
        } else {
          retryCount = retryCount + 1
          failedMessages = messages
          scheduleOnce(NotUsed, settings.retryInterval)
        }
      }

      private def handleResponse(args: (immutable.Seq[OrientDbWriteMessage[T, C]], Option[String])): Unit =
        push(out, failedMessages)

      private def sendOSQLBulkInsertRequest(messages: immutable.Seq[OrientDbWriteMessage[T, C]]): Unit =
        try {
          ODatabaseRecordThreadLocal.instance().set(client)
          if (clazz.isEmpty) {
            if (!client.getMetadata.getSchema.existsClass(className)) {
              client.getMetadata.getSchema.createClass(className)
            }
            client.begin(OTransaction.TXTYPE.OPTIMISTIC)

            var faultyMessages: List[OrientDbWriteMessage[T, C]] = List()
            var successfulMessages: List[OrientDbWriteMessage[T, C]] = List()
            messages.foreach {
              case OrientDbWriteMessage(oDocument: ODocument, passThrough: C) =>
                val document = new ODocument()
                oDocument
                  .asInstanceOf[ODocument]
                  .fieldNames()
                  .zip(oDocument.asInstanceOf[ODocument].fieldValues())
                  .foreach {
                    case (fieldName, fieldVal) =>
                      document.field(fieldName, fieldVal)
                      ()
                  }
                document.setClassName(className)
                client.save(document)
                successfulMessages = successfulMessages ++ List(
                  OrientDbWriteMessage(oDocument.asInstanceOf[T], passThrough)
                )
                ()
              case OrientDbWriteMessage(oRecord: ORecord, passThrough: C) =>
                client.save(oRecord)
                successfulMessages = successfulMessages ++ List(
                  OrientDbWriteMessage(oRecord.asInstanceOf[T], passThrough)
                )
                ()
              case OrientDbWriteMessage(others: AnyRef, passThrough: C) =>
                faultyMessages = faultyMessages ++ List(OrientDbWriteMessage(others.asInstanceOf[T], passThrough))
                ()
            }
            client.commit()
            if (faultyMessages.nonEmpty) {
              handleResponse((faultyMessages, Some("Records are invalid OrientDB Records")))
            } else {
              emit(out, successfulMessages)
              handleResponse((immutable.Seq.empty, None))
            }
          } else {
            client.setDatabaseOwner(oObjectClient)
            oObjectClient.getEntityManager.registerEntityClass(
              clazz.getOrElse(throw new RuntimeException("Typed stream class is invalid"))
            )

            var faultyMessages: List[OrientDbWriteMessage[T, C]] = List()
            var successfulMessages: List[OrientDbWriteMessage[T, C]] = List()
            messages.foreach {
              case OrientDbWriteMessage(typeRecord: T, passThrough: C) =>
                oObjectClient.save(typeRecord)
                successfulMessages = successfulMessages ++ List(OrientDbWriteMessage(typeRecord, passThrough))
                ()
              case OrientDbWriteMessage(others: AnyRef, passThrough: C) =>
                faultyMessages = faultyMessages ++ List(OrientDbWriteMessage(others.asInstanceOf[T], passThrough))
                ()
            }

            if (faultyMessages.nonEmpty) {
              handleResponse((faultyMessages, Some("Records are invalid OrientDB Records")))
            } else {
              emit(out, successfulMessages)
              handleResponse((immutable.Seq.empty, None))
            }
          }
        } catch {
          case exception: Exception =>
            handleFailure((messages, exception))
        }

      setHandlers(in, out, this)

      override def onPull(): Unit = tryPull(in)

      override def onPush(): Unit = {
        val messages = grab(in)
        sendOSQLBulkInsertRequest(messages)
        tryPull(in)
      }

      override def onUpstreamFailure(exception: Throwable): Unit =
        failStage(exception)

    }
}
