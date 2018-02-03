/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.javadsl

import akka.NotUsed
import akka.stream.alpakka.solr.SolrSourceStage
import akka.stream.javadsl.Source
import org.apache.solr.client.solrj.io.Tuple
import org.apache.solr.client.solrj.io.stream.TupleStream

object SolrSource {

  /**
   * Java API: creates a [[SolrSourceStage]] that consumes as [[Tuple]]
   */
  def fromTupleStream(ts: TupleStream): Source[Tuple, NotUsed] =
    Source.fromGraph(new SolrSourceStage(ts))
}
