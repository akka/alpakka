/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.scaladsl

import java.util.{Date, UUID}

case class Person(id: Int, name: String, birthDate: Date)
case class Animal(id: Int, name: String, owner: Int)

case class Complex(id: UUID, ints: List[Int], dates: List[Date], ids: Set[UUID] = Set())
