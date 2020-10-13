/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.influxdb.{InfluxDbReadSettings}
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import org.influxdb.{InfluxDB, InfluxDBException}
import org.influxdb.dto.{Query, QueryResult}

import scala.collection.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDbSourceStage[T](clazz: Class[T],
                                                     settings: InfluxDbReadSettings,
                                                     influxDB: InfluxDB,
                                                     query: Query
) extends GraphStage[SourceShape[T]] {

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
                                                     settings: InfluxDbReadSettings,
                                                     influxDB: InfluxDB,
                                                     query: Query,
                                                     outlet: Outlet[T],
                                                     shape: SourceShape[T]
) extends InfluxDbBaseSourceLogic[T](influxDB, query, outlet, shape) {

  var resultMapperHelper: AlpakkaResultMapperHelper = _

  override def preStart(): Unit = {
    resultMapperHelper = new AlpakkaResultMapperHelper
    resultMapperHelper.cacheClassFields(clazz)
    super.preStart()
  }
  override def onPull(): Unit =
    this.dataRetrieved match {
      case None => completeStage()
      case Some(queryResult) => {
        for (result <- queryResult.getResults.asScala) {
          if (result.hasError) {
            failStage(new InfluxDBException(result.getError))
          } else {
            for (series <- result.getSeries.asScala) {
              emitMultiple(outlet, resultMapperHelper.parseSeriesAs(clazz, series, settings.precision))
            }
          }
        }
        dataRetrieved = None
      }
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
                                                     shape: SourceShape[QueryResult]
) extends InfluxDbBaseSourceLogic[QueryResult](influxDB, query, outlet, shape) {

  override def onPull(): Unit =
    dataRetrieved match {
      case None => completeStage()
      case Some(queryResult) => {
        emit(outlet, queryResult)
        dataRetrieved = None
      }
    }

  override protected def validateTotalResults: Boolean = true
}

/**
 * Internal API.
 */
@InternalApi
private[impl] sealed abstract class InfluxDbBaseSourceLogic[T](influxDB: InfluxDB,
                                                               query: Query,
                                                               outlet: Outlet[T],
                                                               shape: SourceShape[T]
) extends GraphStageLogic(shape)
    with OutHandler {

  setHandler(outlet, this)

  var queryExecuted: Boolean = false
  var dataRetrieved: Option[QueryResult] = None

  override def preStart(): Unit =
    runQuery()

  private def runQuery() =
    if (!queryExecuted) {
      val queryResult = influxDB.query(query)
      if (!queryResult.hasError) {
        failOnError(queryResult)
        dataRetrieved = Some(queryResult)
      } else {
        failStage(new InfluxDBException(queryResult.getError))
        dataRetrieved = None
      }
      queryExecuted = true
    }

  protected def validateTotalResults: Boolean = false

  private def failOnError(result: QueryResult) =
    if (validateTotalResults) {
      val totalErrors = result.getResults.asScala
        .filter(_.hasError)
        .map(_.getError)
      if (totalErrors.size == result.getResults.size()) {
        val errorMessage = totalErrors.reduceLeft((m1, m2) => m1 + ";" + m2)
        failStage(new InfluxDBException(errorMessage))
      }
    }

}
