/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.scaladsl

import akka.stream.alpakka.neo4j.internal.CypherMarshaller
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class Neo4jFlowSpec extends Neo4jBaseSpec {

  "ReactiveNeo4j" should {
    "write nodes with shapeless" in {

      //#flow-shapeless
      val neo4j = new ReactiveNeo4j(driver)

      val simple = Source(1 to 10)
        .map(i => Person(s"name_$i", height = 160 + 1))
        .via(neo4j.flow[Person]('Person))
        .runWith(Sink.foreach(p => logger.debug(p.toString)))

      //#flow-shapeless
      Await.ready(simple, 10 seconds)

    }

    "write nodes with custom marsaheller" in {

      //#flow

      //#marshaller
      val marshaller = new CypherMarshaller[Person]() {
        override def create(p: Person) = s"CREATE (o:Person { name: '${p.name}', height: ${p.height} }) RETURN o"
      }
      //#marshaller

      val neo4j = new ReactiveNeo4j(driver)

      val simple = Source(1 to 10)
        .map(i => Person(s"custom_$i", height = 160 + 1))
        .via(neo4j.flow[Person](marshaller, 'Person))
        .runWith(Sink.foreach(p => logger.debug(p.toString)))

      //#flow
      Await.ready(simple, 10 seconds)

    }
  }

}
