/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.storage.{BigQueryReadClient, ReadRowsRequest, ReadRowsResponse}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession

object SDKClientSource {

  private val RequestParamsHeader = "x-goog-request-params"

  def read(client: BigQueryReadClient, readSession: ReadSession): Source[ReadRowsResponse.Rows, NotUsed] =
    client
      .readRows()
      .addHeader(RequestParamsHeader, s"read_stream=${readSession.name}")
      .invoke(ReadRowsRequest(readSession.name))
      .map(_.rows)

}
