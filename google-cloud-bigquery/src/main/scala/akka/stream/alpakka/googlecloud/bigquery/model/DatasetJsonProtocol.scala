/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import spray.json.{JsonFormat, RootJsonFormat}

import java.util
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.jdk.OptionConverters._

/**
 * Dataset resource model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource:-dataset BigQuery reference]]
 *
 * @param datasetReference a reference that identifies the dataset
 * @param friendlyName a descriptive name for the dataset
 * @param labels the labels associated with this dataset
 * @param location the geographic location where the dataset should reside
 */
final case class Dataset private (datasetReference: DatasetReference,
                                  friendlyName: Option[String],
                                  labels: Option[Map[String, String]],
                                  location: Option[String]) {

  def getDatasetReference = datasetReference
  def getFriendlyName = friendlyName.toJava
  def getLabels = labels.map(_.asJava).toJava
  def getLocation = location.toJava

  def withDatasetReference(datasetReference: DatasetReference) =
    copy(datasetReference = datasetReference)

  def withFriendlyName(friendlyName: Option[String]) =
    copy(friendlyName = friendlyName)
  def withFriendlyName(friendlyName: util.Optional[String]) =
    copy(friendlyName = friendlyName.toScala)

  def withLabels(labels: Option[Map[String, String]]) =
    copy(labels = labels)
  def withLabels(labels: util.Optional[util.Map[String, String]]) =
    copy(labels = labels.toScala.map(_.asScala.toMap))

  def withLocation(location: util.Optional[String]) =
    copy(location = location.toScala)
}

object Dataset {

  /**
   * Java API: Dataset resource model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource:-dataset BigQuery reference]]
   *
   * @param datasetReference a reference that identifies the dataset
   * @param friendlyName a descriptive name for the dataset
   * @param labels the labels associated with this dataset
   * @param location the geographic location where the dataset should reside
   * @return a [[Dataset]]
   */
  def create(datasetReference: DatasetReference,
             friendlyName: util.Optional[String],
             labels: util.Optional[util.Map[String, String]],
             location: util.Optional[String]) =
    Dataset(datasetReference, friendlyName.toScala, labels.toScala.map(_.asScala.toMap), location.toScala)

  implicit val format: RootJsonFormat[Dataset] = jsonFormat4(apply)
}

/**
 * DatasetReference model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#datasetreference BigQuery reference]]
 *
 * @param datasetId A unique ID for this dataset, without the project name
 * @param projectId The ID of the project containing this dataset
 */
final case class DatasetReference private (datasetId: Option[String], projectId: Option[String]) {

  def getDatasetId = datasetId.toJava
  def getProjectId = projectId.toJava

  def withDatasetId(datasetId: Option[String]) =
    copy(datasetId = datasetId)
  def withDatasetId(datasetId: util.Optional[String]) =
    copy(datasetId = datasetId.toScala)

  def withProjectId(projectId: Option[String]) =
    copy(projectId = projectId)
  def withProjectId(projectId: util.Optional[String]) =
    copy(projectId = projectId.toScala)
}

object DatasetReference {

  /**
   * Java API: DatasetReference model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#datasetreference BigQuery reference]]
   *
   * @param datasetId A unique ID for this dataset, without the project name
   * @param projectId The ID of the project containing this dataset
   * @return a [[DatasetReference]]
   */
  def create(datasetId: util.Optional[String], projectId: util.Optional[String]) =
    DatasetReference(datasetId.toScala, projectId.toScala)

  implicit val format: JsonFormat[DatasetReference] = jsonFormat2(apply)
}

/**
 * DatasetListResponse model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list#response-body BigQuery reference]]
 *
 * @param nextPageToken a token that can be used to request the next results page
 * @param datasets an array of the dataset resources in the project
 */
final case class DatasetListResponse private (nextPageToken: Option[String], datasets: Option[Seq[Dataset]]) {

  def getNextPageToken = nextPageToken.toJava
  def getDatasets = datasets.map(_.asJava).toJava

  def withNextPageToken(nextPageToken: Option[String]) =
    copy(nextPageToken = nextPageToken)
  def withNextPageToken(nextPageToken: util.Optional[String]) =
    copy(nextPageToken = nextPageToken.toScala)

  def withDatasets(datasets: Option[Seq[Dataset]]) =
    copy(datasets = datasets)
  def withDatasets(datasets: util.Optional[util.List[Dataset]]) =
    copy(datasets = datasets.toScala.map(_.asScala.toList))
}

object DatasetListResponse {

  /**
   * Java API: DatasetListResponse model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list#response-body BigQuery reference]]
   *
   * @param nextPageToken a token that can be used to request the next results page
   * @param datasets an array of the dataset resources in the project
   * @return a [[DatasetListResponse]]
   */
  def create(nextPageToken: util.Optional[String], datasets: util.Optional[util.List[Dataset]]) =
    DatasetListResponse(nextPageToken.toScala, datasets.toScala.map(_.asScala.toList))

  implicit val format: RootJsonFormat[DatasetListResponse] = jsonFormat2(apply)
  implicit val paginated: Paginated[DatasetListResponse] = _.nextPageToken
}
