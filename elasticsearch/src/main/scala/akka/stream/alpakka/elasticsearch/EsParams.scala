/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

final class EsParams private (val indexName: String, val typeName: Option[String], val apiVersion: ApiVersion) {
  def withIndexName(value: String): EsParams = {
    require(value != null, "You must define an index name")

    copy(indexName = value)
  }

  def withTypeName(value: String): EsParams = {
    if (apiVersion == ApiVersion.V5) {
      require(value != null && value.trim.nonEmpty, "You must define a type name for ElasticSearch API version V5")
    }
    copy(typeName = Option(value))
  }

  def withApiVersion(value: ApiVersion): EsParams = copy(apiVersion = value)

  def copy(indexName: String = indexName, typeName: Option[String] = typeName, apiVersion: ApiVersion = apiVersion) =
    new EsParams(indexName, typeName, apiVersion)
}

object EsParams {
  val Default = new EsParams(
    indexName = "",
    typeName = None,
    apiVersion = ApiVersion.V5
  )

  /** Scala API */
  def apply(): EsParams = Default

  /** Java API */
  def create(): EsParams = Default

  def V7(indexName: String): EsParams =
    EsParams.apply().withIndexName(indexName).withTypeName(null).withApiVersion(ApiVersion.V7)

  def V5(indexName: String, typeName: String): EsParams =
    EsParams.apply().withIndexName(indexName).withTypeName(typeName).withApiVersion(ApiVersion.V5)
}
