/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.stream.alpakka.elasticsearch._
import scaladsl.{ElasticsearchSourceSettings => ScalaElasticsearchSourceSettings}

final class ElasticsearchSourceSettings(val bufferSize: Int) {

  def this() = this(10)

  def withBufferSize(bufferSize: Int): ElasticsearchSourceSettings =
    new ElasticsearchSourceSettings(bufferSize)

  private[javadsl] def asScala: ScalaElasticsearchSourceSettings =
    ScalaElasticsearchSourceSettings(
      bufferSize = this.bufferSize
    )
}
