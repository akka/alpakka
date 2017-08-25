/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.scaladsl

case class Person(name: String, height: Int, birthDate: Option[String] = None)