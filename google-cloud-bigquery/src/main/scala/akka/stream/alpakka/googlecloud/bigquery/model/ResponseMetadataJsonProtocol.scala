/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.JobReference
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object ResponseMetadataJsonProtocol extends DefaultJsonProtocol {

  final case class ResponseMetadata(jobReference: Option[JobReference],
                                    pageToken: Option[String],
                                    nextPageToken: Option[String],
                                    jobComplete: Option[Boolean])

  implicit val format: RootJsonFormat[ResponseMetadata] = jsonFormat4(ResponseMetadata)
}
