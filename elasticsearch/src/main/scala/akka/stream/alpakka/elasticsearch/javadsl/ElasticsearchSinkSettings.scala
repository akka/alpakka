/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import akka.stream.alpakka.elasticsearch._
import scaladsl.{ElasticsearchSinkSettings => ScalaElasticsearchSinkSettings}

final class ElasticsearchSinkSettings(val bufferSize: Int, val retryInterval: Int, val maxRetry: Int) {

  def this() = this(10, 5000, 100)

  def withBufferSize(bufferSize: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(bufferSize, this.retryInterval, this.maxRetry)

  def withRetryInterval(retryInterval: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize, retryInterval, this.maxRetry)

  def withMaxRetry(maxRetry: Int): ElasticsearchSinkSettings =
    new ElasticsearchSinkSettings(this.bufferSize, this.retryInterval, maxRetry)

  private[javadsl] def asScala: ScalaElasticsearchSinkSettings =
    ScalaElasticsearchSinkSettings(
      bufferSize = this.bufferSize,
      retryInterval = this.retryInterval,
      maxRetry = this.maxRetry
    )

}
