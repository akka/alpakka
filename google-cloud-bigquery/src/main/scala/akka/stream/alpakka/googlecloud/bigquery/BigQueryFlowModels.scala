/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleSession

object BigQueryFlowModels {

  class BigQueryProjectConfig(val projectId: String, val dataset: String, val session: GoogleSession)

}
