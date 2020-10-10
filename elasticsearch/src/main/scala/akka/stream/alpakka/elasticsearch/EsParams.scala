/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

final class EsParams private (val indexName: String, val typeName: Option[String]) {
  override def toString =
    s"""EsParams(indexName=$indexName,typeName=$typeName)"""
}

object EsParams {
  def V7(indexName: String): EsParams = {
    require(indexName != null, "You must define an index name")

    new EsParams(indexName, None)
  }

  def V5(indexName: String, typeName: String): EsParams = {
    require(indexName != null, "You must define an index name")
    require(typeName != null && typeName.trim.nonEmpty, "You must define a type name for ElasticSearch API version V5")

    new EsParams(indexName, Some(typeName))
  }
}
