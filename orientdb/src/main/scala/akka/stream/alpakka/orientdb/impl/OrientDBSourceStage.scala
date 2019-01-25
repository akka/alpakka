/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.orientdb.{OrientDBSourceSettings, OrientDbReadResult}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.command.OCommandResultListener
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.sql.query.{OSQLNonBlockingQuery, OSQLSynchQuery}

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
 * INTERNAL API
 */
@InternalApi
private[orientdb] final class OrientDBSourceStage[T](className: String,
                                                     query: Option[String],
                                                     settings: OrientDBSourceSettings,
                                                     clazz: Option[Class[T]] = None)
    extends GraphStage[SourceShape[OrientDbReadResult[T]]] {

  val out: Outlet[OrientDbReadResult[T]] = Outlet("OrientDBSource.out")
  override val shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes) =
    new OrientDBSourceLogic[T](className,
                               query,
                               settings,
                               out,
                               shape,
                               SkipAndLimit(settings.skip, settings.limit),
                               clazz)
}

private[orientdb] sealed class OrientDBSourceLogic[T](className: String,
                                                      query: Option[String],
                                                      settings: OrientDBSourceSettings,
                                                      out: Outlet[OrientDbReadResult[T]],
                                                      shape: SourceShape[OrientDbReadResult[T]],
                                                      skipAndLimit: SkipAndLimit,
                                                      clazz: Option[Class[T]] = None)
    extends GraphStageLogic(shape)
    with OutHandler {

  private val responseHandler = getAsyncCallback[List[T]](handleResponse)

  private var client: ODatabaseDocumentTx = _
  private var oObjectClient: OObjectDatabaseTx = _

  override def preStart(): Unit = {
    client = settings.oDatabasePool.acquire()
    oObjectClient = new OObjectDatabaseTx(client)
  }

  override def postStop(): Unit = {
    ODatabaseRecordThreadLocal.instance().set(client)
    oObjectClient.close()
    client.close()
  }

  def sendOSQLRequest(): Unit =
    if (clazz.isEmpty) {
      ODatabaseRecordThreadLocal.instance().set(client)
      if (query.nonEmpty) {
        client.query[java.util.List[T]](new OSQLNonBlockingQuery[T](query.get, new OSQLCallback))
      } else {
        client.query[java.util.List[T]](
          new OSQLNonBlockingQuery[T](
            s"SELECT * FROM $className SKIP ${skipAndLimit.skip} LIMIT ${skipAndLimit.limit}",
            new OSQLCallback
          )
        )
      }
    } else {
      client.setDatabaseOwner(oObjectClient)
      ODatabaseRecordThreadLocal.instance().set(client)
      oObjectClient.getEntityManager.registerEntityClass(
        clazz.getOrElse(throw new RuntimeException("Typed stream class is invalid"))
      )

      if (query.nonEmpty) {
        oObjectClient.query[java.util.List[T]](new OSQLNonBlockingQuery[T](query.get, new OSQLCallback))
      } else {
        val oDocs =
          oObjectClient
            .query[java.util.List[T]](
              new OSQLSynchQuery[T](
                s"SELECT * FROM $className SKIP ${skipAndLimit.skip} LIMIT ${skipAndLimit.limit}"
              )
            )
            .asScala
            .toList
        if (oDocs.nonEmpty) {
          skipAndLimit.skip = skipAndLimit.skip + skipAndLimit.limit
        }
        handleResponse(oDocs)
      }
    }

  def handleResponse(res: List[T]): Unit =
    if (res.isEmpty)
      completeStage()
    else
      emitMultiple(out, res.map(OrientDbReadResult(_)).toIterator)

  setHandler(out, this)

  override def onPull(): Unit = sendOSQLRequest()

  class OSQLCallback extends OCommandResultListener {
    var oDocs: List[T] = List()

    override def result(iRecord: scala.Any): Boolean = {
      oDocs :+= iRecord.asInstanceOf[T]
      true
    }

    override def getResult: AnyRef = oDocs

    override def end(): Unit = {
      if (oDocs.nonEmpty) {
        skipAndLimit.skip = skipAndLimit.skip + skipAndLimit.limit
      }
      responseHandler.invoke(oDocs)
    }
  }
}

private[orientdb] case class SkipAndLimit(var skip: Int, var limit: Int)
