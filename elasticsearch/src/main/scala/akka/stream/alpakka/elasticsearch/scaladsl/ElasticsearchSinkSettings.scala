/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

//#sink-settings
final case class ElasticsearchSinkSettings(bufferSize: Int = 10,
                                           retryInterval: Int = 5000,
                                           maxRetry: Int = 100,
                                           retryPartialFailure: Boolean = true)
//#sink-settings
