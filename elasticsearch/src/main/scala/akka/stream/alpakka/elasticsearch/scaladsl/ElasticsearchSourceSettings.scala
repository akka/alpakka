/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

/**
 * Scala API to configure Elastiscsearch sources.
 */
//#source-settings
final case class ElasticsearchSourceSettings(bufferSize: Int = 10)
//#source-settings
