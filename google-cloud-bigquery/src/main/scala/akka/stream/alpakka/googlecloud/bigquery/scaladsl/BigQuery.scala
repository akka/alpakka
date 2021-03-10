/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings
import akka.stream.alpakka.googlecloud.bigquery.impl.BigQueryExt

/**
 * Scala API to interface with BigQuery.
 */
@ApiMayChange(issue = "https://github.com/akka/alpakka/issues/2353")
object BigQuery
    extends BigQueryRest
    with BigQueryDatasets
    with BigQueryJobs
    with BigQueryQueries
    with BigQueryTables
    with BigQueryTableData {

  /**
   * Returns the default [[BigQuerySettings]].
   */
  def settings(implicit system: ClassicActorSystemProvider): BigQuerySettings = BigQueryExt(system).settings

  /**
   * Returns the [[BigQuerySettings]] defined at a path in the configuration.
   *
   * @param path the configuration path
   */
  def settings(path: String)(implicit system: ClassicActorSystemProvider): BigQuerySettings =
    BigQueryExt(system).settings(path)

}
