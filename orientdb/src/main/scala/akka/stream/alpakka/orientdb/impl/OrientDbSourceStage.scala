/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.impl

import java.util

import akka.annotation.InternalApi
import akka.stream.alpakka.orientdb.{OrientDbReadResult, OrientDbSourceSettings}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.command.OCommandResultListener
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.sql.query.{OSQLNonBlockingQuery, OSQLSynchQuery}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * INTERNAL API
 */
@InternalApi
private[orientdb] final class OrientDbSourceStage[T](className: String,
                                                     query: Option[String],
                                                     settings: OrientDbSourceSettings,
                                                     clazz: Option[Class[T]] = None)
    extends GraphStage[SourceShape[OrientDbReadResult[T]]] {

  val out: Outlet[OrientDbReadResult[T]] = Outlet("OrientDBSource.out")
  override val shape = SourceShape(out)
  override def initialAttributes: Attributes = super.initialAttributes and ActorAttributes.IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      private val responseHandler = getAsyncCallback[List[T]](handleResponse)

      private var client: ODatabaseDocumentTx = _
      private var oObjectClient: OObjectDatabaseTx = _
      private var skip = settings.skip

      override def preStart(): Unit = {
        client = settings.oDatabasePool.acquire()
        oObjectClient = new OObjectDatabaseTx(client)
      }

      override def postStop(): Unit =
        if (client != null) {
          ODatabaseRecordThreadLocal.instance().set(client)
          if (oObjectClient != null) oObjectClient.close()
          client.close()
        }

      setHandler(out, this)

      override def onPull(): Unit = {
        ODatabaseRecordThreadLocal.instance().set(client)
        clazz match {
          case None => queryORecords()
          case Some(c) =>
            queryTyped(c)
        }
      }

      private def queryTyped(clazz: Class[T]): Unit = {
        client.setDatabaseOwner(oObjectClient)
        oObjectClient.getEntityManager.registerEntityClass(clazz)
        if (query.nonEmpty) {
          oObjectClient.query[util.List[T]](new OSQLNonBlockingQuery[T](query.get, new OSQLCallback))
        } else {
          val oDocs =
            oObjectClient
              .query[util.List[T]](
                new OSQLSynchQuery[T](
                  s"SELECT * FROM $className SKIP ${skip} LIMIT ${settings.limit}"
                )
              )
              .asScala
              .toList
          if (oDocs.nonEmpty) {
            skip += settings.limit
          }
          handleResponse(oDocs)
        }
      }

      private def queryORecords(): Unit =
        if (query.nonEmpty) {
          client.query[util.List[T]](new OSQLNonBlockingQuery[T](query.get, new OSQLCallback))
        } else {
          client.query[util.List[T]](
            new OSQLNonBlockingQuery[T](
              s"SELECT * FROM $className SKIP ${skip} LIMIT ${settings.limit}",
              new OSQLCallback
            )
          )
        }

      private def handleResponse(res: List[T]): Unit =
        if (res.isEmpty)
          completeStage()
        else
          emitMultiple(out, res.map(OrientDbReadResult(_)).toIterator)

      final private class OSQLCallback extends OCommandResultListener {
        val oDocs = ListBuffer.newBuilder[T]

        override def result(iRecord: scala.Any): Boolean = {
          oDocs += iRecord.asInstanceOf[T]
          true
        }

        override def getResult: AnyRef = oDocs

        override def end(): Unit = {
          val res = oDocs.result().toList
          if (res.nonEmpty) {
            skip += settings.limit
          }
          responseHandler.invoke(res)
        }
      }
    }
}
