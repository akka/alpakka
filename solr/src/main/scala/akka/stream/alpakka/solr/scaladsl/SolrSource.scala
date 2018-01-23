/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.scaladsl

import akka.NotUsed
import akka.stream.alpakka.solr.SolrSourceStage
import akka.stream.scaladsl.Source
import org.apache.solr.client.solrj.io.Tuple
import org.apache.solr.client.solrj.io.stream.TupleStream

object SolrSource {

  /**
   * Scala API: creates a [[SolrSourceStage]] that consumes as [[Tuple]]
   */
  def create(collection: String, tupleStream: TupleStream): Source[Tuple, NotUsed] =
    Source.fromGraph(new SolrSourceStage(collection, tupleStream))
}
