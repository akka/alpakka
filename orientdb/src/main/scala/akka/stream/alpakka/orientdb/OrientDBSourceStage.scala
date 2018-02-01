/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.orientechnologies.orient.`object`.db.OObjectDatabaseTx
import com.orientechnologies.orient.core.command.OCommandResultListener
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.sql.query.{OSQLNonBlockingQuery, OSQLSynchQuery}

import scala.collection.JavaConverters._

final case class OOutgoingMessage[T](oDocument: T)

case class OSQLResponse[T](error: Option[String], result: Option[OSQLResult[T]])
case class OSQLResult[T](records: Seq[OOutgoingMessage[T]])

trait MessageReader[T] {
  def convert(oDocs: List[T]): OSQLResponse[T]
}

private[orientdb] final class OrientDBSourceStage[T](className: String,
                                                     query: Option[String],
                                                     settings: OrientDBSourceSettings,
                                                     reader: MessageReader[T],
                                                     clazz: Option[Class[T]] = None)
    extends GraphStage[SourceShape[OOutgoingMessage[T]]] {

  val out: Outlet[OOutgoingMessage[T]] = Outlet("OrientDBSource.out")
  override val shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes) =
    new OrientDBSourceLogic[T](className,
                               query,
                               settings,
                               out,
                               shape,
                               reader,
                               SkipAndLimit(settings.skip, settings.limit),
                               clazz)
}

private[orientdb] sealed class OrientDBSourceLogic[T](className: String,
                                                      query: Option[String],
                                                      settings: OrientDBSourceSettings,
                                                      out: Outlet[OOutgoingMessage[T]],
                                                      shape: SourceShape[OOutgoingMessage[T]],
                                                      reader: MessageReader[T],
                                                      skipAndLimit: SkipAndLimit,
                                                      clazz: Option[Class[T]] = None)
    extends GraphStageLogic(shape)
    with OutHandler {

  private val responseHandler = getAsyncCallback[List[T]](handleResponse)
  private val failureHandler = getAsyncCallback[Throwable](handleFailure)

  private var client: ODatabaseDocumentTx = _
  private var oObjectClient: OObjectDatabaseTx = _

  override def preStart(): Unit = {
    client = settings.oDatabasePool.acquire()
    oObjectClient = new OObjectDatabaseTx(client)
  }

  override def postStop(): Unit = {
    oObjectClient.close()
    client.close()
  }

  def sendOSQLRequest(): Unit =
    if (clazz.isEmpty) {
      ODatabaseRecordThreadLocal.instance().set(client)
      try {
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
      } catch {
        case ex: Exception => handleFailure(ex)
      }
    } else {
      client.setDatabaseOwner(oObjectClient)
      ODatabaseRecordThreadLocal.instance().set(client)
      oObjectClient.getEntityManager.registerEntityClass(
        clazz.getOrElse(throw new RuntimeException("Typed stream class is invalid"))
      )

      try {
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
          onSuccess(oDocs)
        }
      } catch {
        case ex: Exception => handleFailure(ex)
      }
    }

  def onFailure(exception: Exception) = failureHandler.invoke(exception)
  def onSuccess(response: List[T]) = responseHandler.invoke(response)

  def handleFailure(ex: Throwable): Unit =
    failStage(ex)

  def handleResponse(res: List[T]): Unit =
    reader.convert(res) match {
      case OSQLResponse(Some(error), _) =>
        failStage(new IllegalStateException(error))
      case OSQLResponse(None, Some(result)) if result.records.isEmpty =>
        completeStage()
      case OSQLResponse(_, Some(result)) =>
        emitMultiple(out, result.records.toIterator)
    }

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
      onSuccess(oDocs)
    }
  }
}

private[orientdb] case class SkipAndLimit(var skip: Int, var limit: Int)
