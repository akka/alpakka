/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.opensearch

import java.util.concurrent.TimeUnit

import akka.stream.alpakka.common.SourceSettings
import akka.stream.alpakka.elasticsearch.ElasticsearchConnectionSettings

import scala.concurrent.duration.FiniteDuration

/**
 * Configure Opensearch sources.
 *
 */
final class OpensearchSourceSettings private (connection: ElasticsearchConnectionSettings,
                                                 bufferSize: Int,
                                                 includeDocumentVersion: Boolean,
                                                 scrollDuration: FiniteDuration,
                                                 apiVersion: ApiVersion) 
                                     extends SourceSettings[ApiVersion, OpensearchSourceSettings](connection, bufferSize, includeDocumentVersion, scrollDuration, apiVersion)  {
  protected override def copy(connection: ElasticsearchConnectionSettings,
                   bufferSize: Int,
                   includeDocumentVersion: Boolean,
                   scrollDuration: FiniteDuration,
                   apiVersion: ApiVersion): OpensearchSourceSettings =
    new OpensearchSourceSettings(connection = connection,
                                    bufferSize = bufferSize,
                                    includeDocumentVersion = includeDocumentVersion,
                                    scrollDuration = scrollDuration,
                                    apiVersion = apiVersion)

  override def toString =
    s"""OpensearchSourceSettings(connection=$connection,bufferSize=$bufferSize,includeDocumentVersion=$includeDocumentVersion,scrollDuration=$scrollDuration,apiVersion=$apiVersion)"""

}

object OpensearchSourceSettings {
  /** Scala API */
  def apply(connection: ElasticsearchConnectionSettings): OpensearchSourceSettings =
    new OpensearchSourceSettings(connection,
                                    10,
                                    includeDocumentVersion = false,
                                    FiniteDuration(5, TimeUnit.MINUTES),
                                    ApiVersion.V1)

  /** Java API */
  def create(connection: ElasticsearchConnectionSettings): OpensearchSourceSettings =
    new OpensearchSourceSettings(connection,
                                    10,
                                    includeDocumentVersion = false,
                                    FiniteDuration(5, TimeUnit.MINUTES),
                                    ApiVersion.V1)
}
