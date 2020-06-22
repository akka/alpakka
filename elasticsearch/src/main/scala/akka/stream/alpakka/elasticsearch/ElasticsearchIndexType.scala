/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

case class ElasticsearchIndexType(indexName: String, typeName: Option[String], apiVersion: ApiVersion) {
  require(indexName != null, "You must define an index name")

  if (apiVersion == ApiVersion.V5) {
    require(typeName.exists(_.trim.nonEmpty), "You must define a type name")
  }
}
