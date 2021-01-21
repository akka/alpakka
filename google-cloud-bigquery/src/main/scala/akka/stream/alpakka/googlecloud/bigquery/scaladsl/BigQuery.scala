/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.actor.ClassicActorSystemProvider
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings
import akka.stream.alpakka.googlecloud.bigquery.impl.BigQueryExt

/**
 * Scala API to interface with BigQuery.
 */
object BigQuery
    extends BigQueryRest
    with BigQueryDatasets
    with BigQueryJobs
    with BigQueryQueries
    with BigQueryTables
    with BigQueryTableData {

  def settings(implicit system: ClassicActorSystemProvider): BigQuerySettings = BigQueryExt(system).settings

  def settings(prefix: String)(implicit system: ClassicActorSystemProvider): BigQuerySettings =
    BigQueryExt(system).settings(prefix)

}
