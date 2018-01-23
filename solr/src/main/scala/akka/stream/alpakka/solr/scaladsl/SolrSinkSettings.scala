/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.scaladsl

//#sink-settings
final case class SolrSinkSettings(bufferSize: Int = 10,
                                  retryInterval: Int = 5000,
                                  maxRetry: Int = 100,
                                  commitWithin: Int = -1)
//#sink-settings
