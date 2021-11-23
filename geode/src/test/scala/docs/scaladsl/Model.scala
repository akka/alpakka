/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.util.{Date, UUID}

case class Person(id: Int, name: String, birthDate: Date)
case class Animal(id: Int, name: String, owner: Int)

case class Complex(id: UUID, ints: List[Int], dates: List[Date], ids: Set[UUID] = Set())
