/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.xml

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.xml.pull.{ EvElemEnd, EvElemStart, EvText, XMLEvent }

object PersonParser {
  val flow: Flow[XMLEvent, Person, NotUsed] = {
    import akka.stream.alpakka.xml.XmlParser._
    var person = Person()
    var address = Address()
    var inFirstName = false
    var inLastName = false
    var inAge = false
    var inStreet = false
    var inHouseNr = false
    var inZip = false
    var inCity = false

    XmlParser.flow {
      case EvElemStart(_, "first-name", _, _) =>
        inFirstName = true; emit()
      case EvElemStart(_, "last-name", _, _) =>
        inLastName = true; emit()
      case EvElemStart(_, "age", _, _) =>
        inAge = true; emit()

      case EvText(text) if inFirstName =>
        person = person.copy(firstName = text); emit()
      case EvText(text) if inLastName =>
        person = person.copy(lastName = text); emit()
      case EvText(text) if inAge =>
        person = person.copy(age = text.toInt); emit()

      case EvElemEnd(_, "first-name") =>
        inFirstName = false; emit()
      case EvElemEnd(_, "last-name") =>
        inLastName = false; emit()
      case EvElemEnd(_, "age") =>
        inAge = false; emit()
      case EvElemStart(_, "street", _, _) =>
        inStreet = true; emit()
      case EvElemStart(_, "house-number", _, _) =>
        inHouseNr = true; emit()
      case EvElemStart(_, "zip-code", _, _) =>
        inZip = true; emit()
      case EvElemStart(_, "city", _, _) =>
        inCity = true; emit()
      case EvElemEnd(_, "street") =>
        inStreet = false; emit()
      case EvElemEnd(_, "house-number") =>
        inHouseNr = false; emit()
      case EvElemEnd(_, "zip-code") =>
        inZip = false; emit()
      case EvElemEnd(_, "city") =>
        inCity = false; emit()

      case EvText(text) if inStreet =>
        address = address.copy(street = text); emit()
      case EvText(text) if inHouseNr =>
        address = address.copy(houseNumber = text); emit()
      case EvText(text) if inZip =>
        address = address.copy(zipCode = text); emit()
      case EvText(text) if inCity =>
        address = address.copy(city = text); emit()

      case EvElemEnd(_, "person") =>
        val iter = emit(person.copy(address = address))
        person = Person()
        address = Address()
        iter
    }
  }
}
