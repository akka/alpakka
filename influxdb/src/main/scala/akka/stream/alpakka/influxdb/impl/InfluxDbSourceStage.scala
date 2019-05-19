/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.influxdb.InfluxDbSettings
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import org.influxdb.{InfluxDB, InfluxDBException}
import org.influxdb.dto.{Query, QueryResult}
import org.influxdb.impl.InfluxDbResultMapperHelper

import scala.collection.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDbSourceStage[T](clazz: Class[T],
                                                     settings: InfluxDbSettings,
                                                     influxDB: InfluxDB,
                                                     query: Query)
    extends GraphStage[SourceShape[T]] {

  val out: Outlet[T] = Outlet("InfluxDb.out")
  override val shape = SourceShape(out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes(ActorAttributes.IODispatcher)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new InfluxDbSourceLogic[T](clazz, settings, influxDB, query, out, shape)

}

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDbSourceLogic[T](clazz: Class[T],
                                                     settings: InfluxDbSettings,
                                                     influxDB: InfluxDB,
                                                     query: Query,
                                                     outlet: Outlet[T],
                                                     shape: SourceShape[T])
    extends GraphStageLogic(shape)
    with OutHandler {

  setHandler(outlet, this)

  var queryExecuted: Boolean = false
  var dataRetrieved: Option[QueryResult] = None
  var resultMapperHelper: InfluxDbResultMapperHelper = _

  override def preStart(): Unit = {
    resultMapperHelper = new InfluxDbResultMapperHelper
    resultMapperHelper.cacheClassFields(clazz)
  }

  override def onPull(): Unit = {
    runQuery()
    dataRetrieved match {
      case None => completeStage()
      case Some(queryResult) => {
        for (result <- queryResult.getResults.asScala) {
          if (result.hasError) {
            failStage(new InfluxDBException(result.getError))
          } else {
            for (series <- result.getSeries.asScala) {
              emitMultiple(outlet,
                           resultMapperHelper.parseSeriesAs(clazz, series, settings.precision).asScala.toIterator)
            }
          }
        }
        dataRetrieved = None
      }
    }
  }

  private def runQuery() =
    if (!queryExecuted) {
      val queryResult = influxDB.query(query)
      if (!queryResult.hasError) {
        dataRetrieved = Some(queryResult)
      } else {
        failStage(new InfluxDBException(queryResult.getError))
        dataRetrieved = None
      }
      queryExecuted = true
    }

}

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDbRawSourceStage(query: Query, influxDB: InfluxDB)
    extends GraphStage[SourceShape[QueryResult]] {

  val out: Outlet[QueryResult] = Outlet("InfluxDb.out")
  override val shape = SourceShape(out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes(ActorAttributes.IODispatcher)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new InfluxDbSourceRawLogic(query, influxDB, out, shape)

}

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDbSourceRawLogic(query: Query,
                                                     influxDB: InfluxDB,
                                                     outlet: Outlet[QueryResult],
                                                     shape: SourceShape[QueryResult])
    extends GraphStageLogic(shape)
    with OutHandler {

  setHandler(outlet, this)

  var queryExecuted: Boolean = false
  var dataRetrieved: Option[QueryResult] = None

  override def onPull(): Unit = {
    runQuery()
    dataRetrieved match {
      case None => completeStage()
      case Some(queryResult) => {
        emit(outlet, queryResult)
        dataRetrieved = None
      }
    }
  }

  private def runQuery() =
    if (!queryExecuted) {
      val queryResult = influxDB.query(query)
      if (!queryResult.hasError) {
        queryResult.getResults.forEach(failOnError)
        dataRetrieved = Some(queryResult)
      } else {
        failStage(new InfluxDBException(queryResult.getError))
        dataRetrieved = None
      }
      queryExecuted = true
    }

  private def failOnError(result: QueryResult.Result) =
    if (result.hasError) {
      failStage(new InfluxDBException(result.getError))
    }

}
