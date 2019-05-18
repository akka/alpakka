/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.InfluxDbSettings
import akka.stream.javadsl.Source
import org.influxdb.InfluxDB
import org.influxdb.dto.{Query, QueryResult}
import akka.stream.alpakka.influxdb.impl.{InfluxDbRawSourceStage, InfluxDbSourceStage}

/**
 * Java API to create InfluxDB sources.
 */
object InfluxDbSource {

  /**
   * Java API: creates an [[InfluxDbRawSourceStage]] from a given statement.
   */
  def create(influxDB: InfluxDB, query: Query): Source[QueryResult, NotUsed] =
    Source.fromGraph(new InfluxDbRawSourceStage(query, influxDB))

  /**
   * Java API: creates an  [[InfluxDbSourceStage]] of elements of `T` from `query`.
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
