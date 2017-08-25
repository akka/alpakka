/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.internal

import org.neo4j.driver.v1.Driver

abstract class Neo4jCapabilities(val driver: Driver) {
  def close() = driver.close()
}
