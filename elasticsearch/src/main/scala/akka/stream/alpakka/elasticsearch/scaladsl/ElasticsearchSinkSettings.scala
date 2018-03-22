/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

/**
 * Scala API to configure Elasticsearch sinks.
 *
 * Note: If using retryPartialFailure == true, you will receive
 * messages out of order downstream in cases where
 * elastic returns error one some of the documents in a
 * bulk request.
 */
//#sink-settings
final case class ElasticsearchSinkSettings(bufferSize: Int = 10,
                                           retryInterval: Int = 5000,
                                           maxRetry: Int = 100,
                                           retryPartialFailure: Boolean = false,
                                           docAsUpsert: Boolean = false,
                                           versionType: Option[String] = None)
//#sink-settings
