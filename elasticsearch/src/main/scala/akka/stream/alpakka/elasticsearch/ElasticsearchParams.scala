/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

final class ElasticsearchParams private (val indexName: String, val typeName: Option[String]) {
  override def toString =
    s"""ElasticsearchParams(indexName=$indexName,typeName=$typeName)"""
}

object ElasticsearchParams {
  def V7(indexName: String): ElasticsearchParams = {
    require(indexName != null, "You must define an index name")

    new ElasticsearchParams(indexName, None)
  }

  def V5(indexName: String, typeName: String): ElasticsearchParams = {
    require(indexName != null, "You must define an index name")
    require(typeName != null && typeName.trim.nonEmpty, "You must define a type name for ElasticSearch API version V5")

    new ElasticsearchParams(indexName, Some(typeName))
  }
}
