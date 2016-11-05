/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.xml

import scala.concurrent.duration._

class PersonXmlProcessorTest extends TestSpec {
  it should "process full person" in {
    withTestXMLPersonParser()(PersonXmlFile) { tp =>
      tp.request(1)
      tp.expectNext(testPerson1)
      tp.expectNoMsg(100.millis)
    }
  }

  it should "count a lot of persons" in {
    withInputStream(PeopleXmlFile) { is =>
      XmlEventSource.fromInputStream(is)
        .via(PersonParser.flow)
        .runFold(0) { case (c, _) => c + 1 }
        .futureValue shouldBe 4400
    }
  }
}
