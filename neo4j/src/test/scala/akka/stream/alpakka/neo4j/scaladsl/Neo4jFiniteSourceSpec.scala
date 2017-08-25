/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.scaladsl

import akka.stream.alpakka.neo4j.internal.CypherUnmarshaller
import akka.stream.scaladsl.Sink
import org.neo4j.driver.v1.Value

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class Neo4jFiniteSourceSpec extends Neo4jBaseSpec {

  "ReactiveNeo4j" should {
    "read nodes with shapeless" in {

      //#source-shapeless
      val neo4j = new ReactiveNeo4j(driver)

      val simple = neo4j
        .source[Person]("MATCH (ee:Person) RETURN ee")
        .runWith(Sink.foreach(p => logger.debug(p.toString)))

      //#source-shapeless
      Await.ready(simple, 10 seconds)

    }

    "read nodes with custom unmarshaller" in {

      //#source

      //#unmarshaller
      val unmarshaller = new CypherUnmarshaller[Person]() {
        override def unmarshall(record: Value) = Person(record.get("name", ""), record.get("height", 0), None);
      }
      //#unmarshaller

      val neo4j = new ReactiveNeo4j(driver)

      val simple = neo4j
        .source[Person](unmarshaller, "MATCH (ee:Person) RETURN ee")
        .runWith(Sink.foreach(p => logger.debug(p.toString)))

      //#source
      Await.ready(simple, 10 seconds)

    }
  }

}
