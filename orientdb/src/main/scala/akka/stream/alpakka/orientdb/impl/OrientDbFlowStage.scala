/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.impl

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.orientdb.{OrientDbWriteMessage, OrientDbWriteSettings}
import akka.stream.stage._
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.tx.OTransaction

import scala.collection.immutable
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi
private[orientdb] class OrientDbFlowStage[T, C](
    className: String,
    settings: OrientDbWriteSettings,
    clazz: Option[Class[T]]
) extends GraphStage[FlowShape[immutable.Seq[OrientDbWriteMessage[T, C]], immutable.Seq[OrientDbWriteMessage[T, C]]]] {

  private val in = Inlet[immutable.Seq[OrientDbWriteMessage[T, C]]]("in")
  private val out = Outlet[immutable.Seq[OrientDbWriteMessage[T, C]]]("out")
  override val shape = FlowShape(in, out)
  override def initialAttributes: Attributes = super.initialAttributes and ActorAttributes.IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    clazz match {
      case Some(c) => new OrientDbTypedLogic(c)
      case None => new ORecordLogic(className)
    }

  sealed abstract class OrientDbLogic extends GraphStageLogic(shape) with InHandler with OutHandler {

    protected var client: ODatabaseDocumentTx = _
    protected var oObjectClient: OObjectDatabaseTx = _

    override def preStart(): Unit = {
      client = settings.oDatabasePool.acquire()
      oObjectClient = new OObjectDatabaseTx(client)
    }

    override def postStop(): Unit = {
      ODatabaseRecordThreadLocal.instance().set(client)
      oObjectClient.close()
      client.close()
    }

    protected def sendOSQLBulkInsertRequest(messages: immutable.Seq[OrientDbWriteMessage[T, C]]): Unit

    setHandlers(in, out, this)

    override def onPull(): Unit = if (!isClosed(in) && !hasBeenPulled(in)) pull(in)

    override def onPush(): Unit = {
      val messages = grab(in)
      sendOSQLBulkInsertRequest(messages)
      tryPull(in)
    }

  }

  final class ORecordLogic(className: String) extends OrientDbLogic {

    protected def sendOSQLBulkInsertRequest(messages: immutable.Seq[OrientDbWriteMessage[T, C]]): Unit = {
      ODatabaseRecordThreadLocal.instance().set(client)
      if (!client.getMetadata.getSchema.existsClass(className)) {
        client.getMetadata.getSchema.createClass(className)
      }
      client.begin(OTransaction.TXTYPE.OPTIMISTIC)
      try {
        messages.foreach {
          case OrientDbWriteMessage(oDocument: ODocument, _) =>
            val document = new ODocument()
            oDocument
              .asInstanceOf[ODocument]
              .fieldNames()
              .zip(oDocument.asInstanceOf[ODocument].fieldValues())
              .foreach {
                case (fieldName, fieldVal) =>
                  document.field(fieldName, fieldVal)
              }
            document.setClassName(className)
            client.save(document)
            ()
          case OrientDbWriteMessage(oRecord: ORecord, _) =>
            client.save(oRecord)
            ()
          case OrientDbWriteMessage(others: AnyRef, _) =>
            failStage(new RuntimeException(s"unexpected type [${others.getClass()}], ORecord required"))
        }
        client.commit()
        push(out, messages)
      } catch {
        case NonFatal(e) =>
          client.rollback()
          throw e
      }
    }
  }

  final class OrientDbTypedLogic(clazz: Class[T]) extends OrientDbLogic() {

    protected def sendOSQLBulkInsertRequest(messages: immutable.Seq[OrientDbWriteMessage[T, C]]): Unit = {
      ODatabaseRecordThreadLocal.instance().set(client)
      client.setDatabaseOwner(oObjectClient)
      oObjectClient.getEntityManager.registerEntityClass(clazz)
      client.begin(OTransaction.TXTYPE.OPTIMISTIC)
      try {
        messages.foreach {
          case OrientDbWriteMessage(typeRecord: Any, _) =>
            oObjectClient.save(typeRecord)
            ()
        }
        client.commit()
        push(out, messages)
      } catch {
        case NonFatal(e) =>
          client.rollback()
          throw e
      }
    }

  }
}
