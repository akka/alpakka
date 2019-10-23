/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.client

import spray.json.{DefaultJsonProtocol, JsonFormat}

object DryRunJsonProtocol extends DefaultJsonProtocol {

  case class DryRunResponse(totalBytesProcessed: String, jobComplete: Boolean, cacheHit: Boolean)

  implicit val dryRunFormat: JsonFormat[DryRunResponse] = jsonFormat3(DryRunResponse)
}
