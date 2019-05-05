package akka.stream.alpakka.influxdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.InfluxDBSettings
import akka.stream.javadsl.Source
import org.influxdb.InfluxDB
import org.influxdb.dto.{Query, QueryResult}
import akka.stream.alpakka.influxdb.impl.{InfluxDBRawSourceStage, InfluxDBSourceStage}

/**
 * Java API to create InfluxDB sources.
 */
object InfluxDBSource {

  /**
   * Java API: creates an [[InfluxDBRawSourceStage]] from a given statement.
   */
  def create(influxDB: InfluxDB, query: Query): Source[QueryResult, NotUsed] =
    Source.fromGraph(new InfluxDBRawSourceStage(query, influxDB))

  /**
   * Java API: creates an  [[InfluxDBSourceStage]] of elements of `T` from `query`.
   */
  def typed[T](clazz: Class[T],
               settings: InfluxDBSettings,
               influxDB: InfluxDB,
               query: Query
              ): Source[T, NotUsed] =
    Source.fromGraph(
      new InfluxDBSourceStage[T](
        clazz,
        settings,
        influxDB,
        query
      )
    )


}
