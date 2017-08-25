/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.scaladsl.codec

import akka.stream.alpakka.neo4j.internal.codec.Neo4jEncoder
import akka.stream.alpakka.neo4j.scaladsl.Person
import org.scalatest.WordSpec

class Neo4jCodecSpec extends WordSpec {

  "coded" should {
    "enoode" in {

      val writer = new StringBuilder

      Neo4jEncoder[Person].encode(writer, None, Person("Agnes", 162, birthDate = Some("dd")))

      println(writer.toString())
    }
  }

}
