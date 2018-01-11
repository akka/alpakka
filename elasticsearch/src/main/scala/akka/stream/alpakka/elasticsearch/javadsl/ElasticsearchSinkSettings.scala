/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.stream.alpakka.elasticsearch._
import scaladsl.{ElasticsearchSinkSettings => ScalaElasticsearchSinkSettings}

/**
 * Java API to configure Elasticsearch sinks.
 */
final class ElasticsearchSinkSettings(val bufferSize: Int,
                                      val retryInterval: Int,
                                      val maxRetry: Int,
                                      val retryPartialFailure: Boolean,
                                      val docAsUpsert: Boolean) {

  def this() = this(10, 5000, 100, true, false)

  def withBufferSize(bufferSize: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(bufferSize,
                                  this.retryInterval,
                                  this.maxRetry,
                                  this.retryPartialFailure,
                                  this.docAsUpsert)

  def withRetryInterval(retryInterval: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  retryInterval,
                                  this.maxRetry,
                                  this.retryPartialFailure,
                                  this.docAsUpsert)

  def withMaxRetry(maxRetry: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  this.retryInterval,
                                  maxRetry,
                                  this.retryPartialFailure,
                                  this.docAsUpsert)

  def withRetryPartialFailure(retryPartialFailure: Boolean): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  this.retryInterval,
                                  this.maxRetry,
                                  retryPartialFailure,
                                  this.docAsUpsert)

  def withDocAsUpsert(docAsUpsert: Boolean): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize,
                                  this.retryInterval,
                                  this.maxRetry,
                                  this.retryPartialFailure,
                                  docAsUpsert)

  private[javadsl] def asScala: ScalaElasticsearchSinkSettings =
    ScalaElasticsearchSinkSettings(
      bufferSize = this.bufferSize,
      retryInterval = this.retryInterval,
      maxRetry = this.maxRetry,
      retryPartialFailure = this.retryPartialFailure,
      docAsUpsert = this.docAsUpsert
    )

}
