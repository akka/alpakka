/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr.javadsl

import akka.NotUsed
import akka.stream.alpakka.solr.impl.SolrSourceStage
import akka.stream.javadsl.Source
import org.apache.solr.client.solrj.io.Tuple
import org.apache.solr.client.solrj.io.stream.TupleStream

/**
 * Java API
 */
object SolrSource {

  /**
   * Use a Solr [[org.apache.solr.client.solrj.io.stream.TupleStream]] as source.
   */
  def fromTupleStream(ts: TupleStream): Source[Tuple, NotUsed] =
    Source.fromGraph(new SolrSourceStage(ts))
}
