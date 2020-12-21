/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

object DatasetJsonProtocol extends DefaultJsonProtocol {

  final case class Dataset(datasetReference: DatasetReference,
                           friendlyName: Option[String],
                           labels: Option[Map[String, String]],
                           location: Option[String])
  final case class DatasetReference(datasetId: String, projectId: Option[String])

  implicit val referenceFormat: JsonFormat[DatasetReference] = jsonFormat2(DatasetReference)
  implicit val format: RootJsonFormat[Dataset] = jsonFormat4(Dataset)

  final case class DatasetListResponse(nextPageToken: Option[String], datasets: Option[Seq[Dataset]])

  implicit val listResponseFormat: RootJsonFormat[DatasetListResponse] = jsonFormat2(DatasetListResponse)
  implicit val paginated: Paginated[DatasetListResponse] = _.nextPageToken
}
