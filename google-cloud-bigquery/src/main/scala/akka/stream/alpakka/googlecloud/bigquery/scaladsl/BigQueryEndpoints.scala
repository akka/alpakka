/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.http.scaladsl.model.Uri

object BigQueryEndpoints extends BigQueryEndpoints(Uri("https://bigquery.googleapis.com/bigquery/v2")) {
  private final implicit class UriWithSlash(val uri: Uri) extends AnyVal {
    def /(segment: String): Uri = uri.withPath(uri.path / segment)
  }
}

object BigQueryMediaEndpoints extends BigQueryEndpoints(Uri("https://bigquery.googleapis.com/upload/bigquery/v2"))

sealed abstract class BigQueryEndpoints(final val endpoint: Uri) {

  import BigQueryEndpoints.UriWithSlash

  final def projects: Uri = endpoint / "projects"

  final def project(projectId: String): Uri = projects / projectId

  final def datasets(projectId: String): Uri = project(projectId) / "datasets"

  final def dataset(projectId: String, datasetId: String): Uri = datasets(projectId) / datasetId

  final def jobs(projectId: String): Uri = project(projectId) / "jobs"

  final def job(projectId: String, jobId: String): Uri = jobs(projectId) / jobId

  final def jobCancel(projectId: String, jobId: String): Uri = job(projectId, jobId) / "cancel"

  final def queries(projectId: String): Uri = project(projectId) / "queries"

  final def query(projectId: String, jobId: String): Uri = queries(projectId) / jobId

  final def tables(projectId: String, datasetId: String): Uri = dataset(projectId, datasetId) / "tables"

  final def table(projectId: String, datasetId: String, tableId: String): Uri = tables(projectId, datasetId) / tableId

  final def tableData(projectId: String, datasetId: String, tableId: String): Uri =
    table(projectId, datasetId, tableId) / "data"

  final def tableDataInsertAll(projectId: String, datasetId: String, tableId: String): Uri =
    table(projectId, datasetId, tableId) / "insertAll"
}
