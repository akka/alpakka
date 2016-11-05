/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.xml

import scala.xml.pull.{ EvElemEnd, EvElemStart, EvText }

class XMLEventFromTrySourceTest extends TestSpec {
  it should "parse persons.xml" in {
    withTestXMLEventSource()(PersonXmlFile) { tp =>
      tp.request(Int.MaxValue)
      tp.expectNext() should matchPattern { case EvElemStart(_, "persons", _, _) => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "person", _, _) => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "first-name", _, _) => }
      tp.expectNext() should matchPattern { case EvText("Barack") => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "first-name") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "last-name", _, _) => }
      tp.expectNext() should matchPattern { case EvText("Obama") => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "last-name") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "age", _, _) => }
      tp.expectNext() should matchPattern { case EvText("54") => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "age") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "address", _, _) => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "street", _, _) => }
      tp.expectNext() should matchPattern { case EvText("Pennsylvania Ave") => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "street") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "house-number", _, _) => }
      tp.expectNext() should matchPattern { case EvText("1600") => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "house-number") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "zip-code", _, _) => }
      tp.expectNext() should matchPattern { case EvText("20500") => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "zip-code") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemStart(_, "city", _, _) => }
      tp.expectNext() should matchPattern { case EvText("Washington") => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "city") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "address") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "person") => }
      tp.expectNext() should matchPattern { case EvText(_) => }
      tp.expectNext() should matchPattern { case EvElemEnd(_, "persons") => }
      tp.expectComplete()
    }
  }
}
