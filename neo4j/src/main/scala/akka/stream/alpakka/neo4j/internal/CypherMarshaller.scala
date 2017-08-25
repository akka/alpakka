/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.internal

trait CypherMarshaller[A] {
  def create(a: A): String
}
