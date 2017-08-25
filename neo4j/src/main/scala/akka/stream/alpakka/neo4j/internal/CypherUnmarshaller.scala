/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.internal

import org.neo4j.driver.v1.Value

trait CypherUnmarshaller[A] {
  def unmarshall(record: Value): A
}
