/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.xml

import akka.stream.scaladsl.Sink

class XsdValidationTest extends TestSpec {
  "XsdValidation sink" should "validate xml/people.xml" in withByteStringSource(PeopleXmlFile) { src =>
    src.runWith(XsdValidation.sink(PeopleXsdFile)).futureValue.status should be a 'success
  }

  it should "validate xml/invalid-persons.xml" in withByteStringSource(InvalidPerson) { src =>
    src.runWith(XsdValidation.sink(PeopleXsdFile)).futureValue.status should be a 'failure
  }

  it should "validate xml/lot-of-persons.xml" in withByteStringSource(PeopleXmlFile) { src =>
    src.runWith(XsdValidation.sink(PeopleXsdFile)).futureValue.status should be a 'success
  }

  "XsdValidation flow" should "validate xml/persons.xml" in withByteStringSource(PeopleXmlFile) { src =>
    src.via(XsdValidation.flow(PeopleXsdFile)).runWith(Sink.head).futureValue.status should be a 'success
  }

  it should "validate xml/invalid-persons.xml" in withByteStringSource(InvalidPerson) { src =>
    src.via(XsdValidation.flow(PeopleXsdFile)).runWith(Sink.head).futureValue.status should be a 'failure
  }

  it should "validate xml/lot-of-persons.xml" in withByteStringSource(PeopleXmlFile) { src =>
    src.via(XsdValidation.flow(PeopleXsdFile)).runWith(Sink.head).futureValue.status should be a 'success
  }
}
