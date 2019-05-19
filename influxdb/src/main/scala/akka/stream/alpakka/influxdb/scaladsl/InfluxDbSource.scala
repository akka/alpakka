/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.InfluxDbSettings
import akka.stream.alpakka.influxdb.impl.{InfluxDbRawSourceStage, InfluxDbSourceStage}
import akka.stream.scaladsl.Source
import org.influxdb.InfluxDB
import org.influxdb.dto.{Query, QueryResult}

/**
 * Scala API.
 */
object InfluxDbSource {

  /**
   * Java API: creates an [[akka.stream.alpakka.influxdb.impl.InfluxDbRawSourceStage]] from a given statement.
   */
  def apply(influxDB: InfluxDB, query: Query): Source[QueryResult, NotUsed] =
    Source.fromGraph(new InfluxDbRawSourceStage(query, influxDB))

  /**
   * Read elements of `T` from `className` or by `query`.
   */
  def typed[T](clazz: Class[T], settings: InfluxDbSettings, influxDB: InfluxDB, query: Query): Source[T, NotUsed] =
    Source.fromGraph(
      new InfluxDbSourceStage[T](
        clazz,
        settings,
        influxDB,
        query
      )
    )

}
