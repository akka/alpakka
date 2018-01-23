/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.javadsl

import akka.stream.alpakka.solr._
import scaladsl.{SolrSinkSettings => ScalaSolrSinkSettings}

final class SolrSinkSettings(val bufferSize: Int, val retryInterval: Int, val maxRetry: Int, val commitWithin: Int) {

  def this() = this(10, 5000, 100, -1)

  def withBufferSize(bufferSize: Int): SolrSinkSettings =
    new SolrSinkSettings(bufferSize, this.retryInterval, this.maxRetry, this.commitWithin)

  def withRetryInterval(retryInterval: Int): SolrSinkSettings =
    new SolrSinkSettings(this.bufferSize, retryInterval, this.maxRetry, this.commitWithin)

  def withMaxRetry(maxRetry: Int): SolrSinkSettings =
    new SolrSinkSettings(this.bufferSize, this.retryInterval, maxRetry, this.commitWithin)

  def withCommitWithin(commitWithin: Int): SolrSinkSettings =
    new SolrSinkSettings(this.bufferSize, this.retryInterval, this.maxRetry, commitWithin)

  private[javadsl] def asScala: ScalaSolrSinkSettings =
    ScalaSolrSinkSettings(
      bufferSize = this.bufferSize,
      retryInterval = this.retryInterval,
      maxRetry = this.maxRetry,
      commitWithin = this.commitWithin
    )

}
