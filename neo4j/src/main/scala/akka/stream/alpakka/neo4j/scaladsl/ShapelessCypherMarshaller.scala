/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.scaladsl

import akka.stream.alpakka.neo4j.internal.CypherMarshaller
import akka.stream.alpakka.neo4j.internal.codec.Neo4jEncoder

//#marshaller
class ShapelessCypherMarshaller[A](label: String, encoder: Neo4jEncoder[A]) extends CypherMarshaller[A] {
  def create(a: A): String = {
    val builder = new StringBuilder
    encoder.encode(builder, None, a)
    val ser = builder.toString()
    s"CREATE( o:$label $ser ) RETURN o"
  }
}
//#marshaller
