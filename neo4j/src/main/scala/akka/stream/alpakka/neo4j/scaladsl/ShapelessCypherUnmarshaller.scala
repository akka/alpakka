/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.scaladsl

import akka.stream.alpakka.neo4j.internal.CypherUnmarshaller
import akka.stream.alpakka.neo4j.internal.codec.Neo4jDecoder
import org.neo4j.driver.v1.Value

//#unmarshaller
class ShapelessCypherUnmarshaller[A](encoder: Neo4jDecoder[A]) extends CypherUnmarshaller[A] {
  def unmarshall(record: Value): A = encoder.decode(record, null).getOrElse(null.asInstanceOf)
}
//#unmarshaller
