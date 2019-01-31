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

    protected def sendOSQLBulkInsertRequest(client: ODatabaseDocumentTx,
                                            messages: immutable.Seq[OrientDbWriteMessage[T, C]]): Unit

    setHandlers(in, out, this)

    override def onPull(): Unit = if (!isClosed(in) && !hasBeenPulled(in)) pull(in)

    override def onPush(): Unit = {
      val messages = grab(in)
      if (messages.nonEmpty) {
        // This is ridiculous, but the docs state
        // "OrientDB supports multi-threads access to the database. ODatabase* and OrientGraph* instances are
        // not thread-safe, so you've to get an instance per thread and each database instance can be used
        // only in one thread per time. The ODocument, OrientVertex and OrientEdge classes are non thread-safe
        // too, so if you share them across threads you can have unexpected errors hard to recognize.
        val client = settings.oDatabasePool.acquire()
        sendOSQLBulkInsertRequest(client, messages)
        client.close()
      }
      tryPull(in)
    }

  }

  final class ORecordLogic(className: String) extends OrientDbLogic {

    protected def sendOSQLBulkInsertRequest(client: ODatabaseDocumentTx,
                                            messages: immutable.Seq[OrientDbWriteMessage[T, C]]): Unit = {
      if (!client.getMetadata.getSchema.existsClass(className)) {
        client.getMetadata.getSchema.createClass(className)
      }
      client.begin(OTransaction.TXTYPE.OPTIMISTIC)
      try {
        messages.foreach {
          case OrientDbWriteMessage(oDocument: ODocument, _) =>
            val document = new ODocument()
            oDocument
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

    protected def sendOSQLBulkInsertRequest(client: ODatabaseDocumentTx,
                                            messages: immutable.Seq[OrientDbWriteMessage[T, C]]): Unit = {
      val oObjectClient = new OObjectDatabaseTx(client)
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
