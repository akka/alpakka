/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.stream.alpakka.elasticsearch._
import scaladsl.{ElasticsearchSinkSettings => ScalaElasticsearchSinkSettings}

/**
 * Java API to configure Elasticsearch sinks.
 *
 * Note: If using retryPartialFailure == true, you will receive
 * messages out of order downstream in cases where
 * elastic returns error one some of the documents in a
 * bulk request.
 */
final class ElasticsearchSinkSettings(val bufferSize: Int,
                                      val retryInterval: Int,
                                      val maxRetry: Int,
                                      val retryPartialFailure: Boolean,
                                      val docAsUpsert: Boolean,
                                      val versionType: Option[String]) {

  def this() = this(10, 5000, 100, false, false, None)

  def withBufferSize(bufferSize: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(bufferSize,
                                  this.retryInterval,
                                  this.maxRetry,
                                  this.retryPartialFailure,
                                  this.docAsUpsert,
                                  this.versionType)

  def withRetryInterval(retryInterval: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  retryInterval,
                                  this.maxRetry,
                                  this.retryPartialFailure,
                                  this.docAsUpsert,
                                  this.versionType)

  def withMaxRetry(maxRetry: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  this.retryInterval,
                                  maxRetry,
                                  this.retryPartialFailure,
                                  this.docAsUpsert,
                                  this.versionType)

  def withRetryPartialFailure(retryPartialFailure: Boolean): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  this.retryInterval,
                                  this.maxRetry,
                                  retryPartialFailure,
                                  this.docAsUpsert,
                                  this.versionType)

  def withDocAsUpsert(docAsUpsert: Boolean): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  this.retryInterval,
                                  this.maxRetry,
                                  this.retryPartialFailure,
                                  docAsUpsert,
                                  this.versionType)

  def withVersionType(versionType: String): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  this.retryInterval,
                                  this.maxRetry,
                                  this.retryPartialFailure,
                                  this.docAsUpsert,
                                  Some(versionType))

  private[javadsl] def asScala: ScalaElasticsearchSinkSettings =
    ScalaElasticsearchSinkSettings(
      bufferSize = this.bufferSize,
      retryInterval = this.retryInterval,
      maxRetry = this.maxRetry,
      retryPartialFailure = this.retryPartialFailure,
      docAsUpsert = this.docAsUpsert,
      versionType = this.versionType
    )

}
