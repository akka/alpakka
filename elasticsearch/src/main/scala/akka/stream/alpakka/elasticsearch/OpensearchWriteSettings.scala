/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

/**
 * Configure Opensearch sinks and flows.
 */
final class OpensearchWriteSettings private (connection: ElasticsearchConnectionSettings,
                                             bufferSize: Int,
                                             retryLogic: RetryLogic,
                                             versionType: Option[String],
                                             apiVersion: OpensearchApiVersion,
                                             allowExplicitIndex: Boolean
) extends WriteSettingsBase[OpensearchApiVersion, OpensearchWriteSettings](connection,
                                                                           bufferSize,
                                                                           retryLogic,
                                                                           versionType,
                                                                           apiVersion,
                                                                           allowExplicitIndex
    ) {

  protected override def copy(connection: ElasticsearchConnectionSettings,
                              bufferSize: Int,
                              retryLogic: RetryLogic,
                              versionType: Option[String],
                              apiVersion: OpensearchApiVersion,
                              allowExplicitIndex: Boolean
  ): OpensearchWriteSettings =
    new OpensearchWriteSettings(connection, bufferSize, retryLogic, versionType, apiVersion, allowExplicitIndex)

  override def toString: String =
    "OpensearchWriteSettings(" +
    s"connection=$connection," +
    s"bufferSize=$bufferSize," +
    s"retryLogic=$retryLogic," +
    s"versionType=$versionType," +
    s"apiVersion=$apiVersion," +
    s"allowExplicitIndex=$allowExplicitIndex)"

}

object OpensearchWriteSettings {

  /** Scala API */
  def apply(connection: ElasticsearchConnectionSettings): OpensearchWriteSettings =
    new OpensearchWriteSettings(connection, 10, RetryNever, None, OpensearchApiVersion.V1, allowExplicitIndex = true)

  /** Java API */
  def create(connection: ElasticsearchConnectionSettings): OpensearchWriteSettings =
    new OpensearchWriteSettings(connection, 10, RetryNever, None, OpensearchApiVersion.V1, allowExplicitIndex = true)
}
