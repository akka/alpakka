/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery

import akka.stream.alpakka.google.cloud.bigquery.impl.GoogleSession

object BigQueryFlowModels {

  class BigQueryProjectConfig(val projectId: String, val dataset: String, val session: GoogleSession)

}
