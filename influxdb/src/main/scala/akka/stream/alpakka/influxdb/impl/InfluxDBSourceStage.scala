package akka.stream.alpakka.influxdb.impl

import java.util.concurrent.TimeUnit

import akka.annotation.InternalApi
import akka.stream.alpakka.influxdb.InfluxDBSettings
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import org.influxdb.InfluxDB
import org.influxdb.dto.{Query, QueryResult}
import org.influxdb.impl.InfluxDBResultMapperHelper

import scala.collection.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDBSourceStage[T](clazz: Class[T],
                                                     settings: InfluxDBSettings,
                                                     influxDB: InfluxDB,
                                                     query: Query,
) extends GraphStage[SourceShape[T]] {

  val out: Outlet[T] = Outlet("InfluxDB.out")
  override val shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new InfluxDBSourceLogic[T](clazz, settings, influxDB, query, out, shape)

}

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDBSourceLogic[T](clazz: Class[T],
                                                     settings: InfluxDBSettings,
                                                     influxDB: InfluxDB,
                                                     query: Query,
                                                     outlet: Outlet[T],
                                                     shape: SourceShape[T])
    extends GraphStageLogic(shape)
    with OutHandler {

  setHandler(outlet, this)

  var dataRetrieved: Option[QueryResult] = Option.empty
  var resultMapperHelper: InfluxDBResultMapperHelper = _

  override def preStart(): Unit = {
    resultMapperHelper = new InfluxDBResultMapperHelper
    resultMapperHelper.cacheClassFields(clazz)

    val queryResult = influxDB.query(query)
    if (!queryResult.hasError) {
      dataRetrieved = Option(queryResult)
    }
  }

  override def onPull(): Unit =
    if (dataRetrieved.isEmpty)
      completeStage()
    else {
      var queryResult = dataRetrieved.get
      for {
        result <- queryResult.getResults.asScala
        series <- result.getSeries.asScala
      }(
        emitMultiple(outlet,
                     resultMapperHelper.parseSeriesAs(clazz, series, settings.precision).asScala.toIterator)
      )

      dataRetrieved = Option.empty
    }

}

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDBRawSourceStage(query: Query, influxDB: InfluxDB)
    extends GraphStage[SourceShape[QueryResult]] {

  val out: Outlet[QueryResult] = Outlet("InfluxDB.out")
  override val shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new InfluxDBSourceRawLogic(query, influxDB, out, shape)

}

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] final class InfluxDBSourceRawLogic(query: Query,
                                                     influxDB: InfluxDB,
                                                     outlet: Outlet[QueryResult],
                                                     shape: SourceShape[QueryResult])
    extends GraphStageLogic(shape)
    with OutHandler {

  setHandler(outlet, this)

  var dataRetrieved: Option[QueryResult] = Option.empty

  override def preStart(): Unit = {
    val queryResult = influxDB.query(query)
    if (!queryResult.hasError) {
      dataRetrieved = Option(queryResult)
    }
  }

  override def onPull(): Unit =
    if (dataRetrieved.isEmpty) {
      completeStage()
    } else {
      emit(outlet, dataRetrieved.get)
      dataRetrieved = Option.empty
    }

}
