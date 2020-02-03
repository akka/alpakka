/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.client

import akka.annotation.InternalApi
import spray.json.{DefaultJsonProtocol, JsonFormat}

@InternalApi
private[bigquery] object DryRunJsonProtocol extends DefaultJsonProtocol {

  case class DryRunResponse(totalBytesProcessed: String, jobComplete: Boolean, cacheHit: Boolean)

  implicit val dryRunFormat: JsonFormat[DryRunResponse] = jsonFormat3(DryRunResponse)
}
