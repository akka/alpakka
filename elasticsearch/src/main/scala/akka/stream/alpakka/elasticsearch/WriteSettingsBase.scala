/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.stream.alpakka.elasticsearch.ElasticsearchConnectionSettings
import akka.stream.alpakka.elasticsearch.RetryLogic

/**
 * Configure Elasticsearch/Opensearch sinks and flows.
 */
abstract class WriteSettingsBase[Version <: ApiVersionBase, W <: WriteSettingsBase[Version, W]] private[alpakka] (
    val connection: ElasticsearchConnectionSettings,
    val bufferSize: Int,
    val retryLogic: RetryLogic,
    val versionType: Option[String],
    val apiVersion: Version,
    val allowExplicitIndex: Boolean
) { this: W =>

  def withConnection(value: ElasticsearchConnectionSettings): W = copy(connection = value)

  def withBufferSize(value: Int): W = copy(bufferSize = value)

  def withRetryLogic(value: RetryLogic): W =
    copy(retryLogic = value)

  def withVersionType(value: String): W = copy(versionType = Option(value))

  def withApiVersion(value: Version): W =
    if (apiVersion == value) this else copy(apiVersion = value)

  def withAllowExplicitIndex(value: Boolean): W = copy(allowExplicitIndex = value)

  protected def copy(connection: ElasticsearchConnectionSettings = connection,
                     bufferSize: Int = bufferSize,
                     retryLogic: RetryLogic = retryLogic,
                     versionType: Option[String] = versionType,
                     apiVersion: Version = apiVersion,
                     allowExplicitIndex: Boolean = allowExplicitIndex): W;
}
