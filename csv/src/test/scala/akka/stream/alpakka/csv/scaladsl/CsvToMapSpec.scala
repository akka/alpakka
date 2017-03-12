/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString

class CsvToMapSpec extends CsvSpec {

  "CSV to Map" should {
    "parse header line and data line into map" in {
      // format: off
      // #header-line
      val flow: Flow[ByteString, Map[String, ByteString], NotUsed]
        = Flow[ByteString]
          .via(CsvFraming.lineScanner())
          .via(CsvToMap.toMap())

      val future =
        Source.single(ByteString(
          """eins,zwei,drei
            |1,2,3
            |""".stripMargin))
          .via(flow)
          .runWith(Sink.head)
      // #header-line
      // format: on
      future.futureValue should be(Map("eins" -> ByteString("1"), "zwei" -> ByteString("2"),
          "drei" -> ByteString("3")))
    }

    "use column names and data line into map" in {
      // format: off
      // #column-names
      val flow: Flow[ByteString, Map[String, ByteString], NotUsed]
        = Flow[ByteString]
          .via(CsvFraming.lineScanner())
          .via(CsvToMap.withHeaders("eins", "zwei", "drei"))

      val future = Source.single(ByteString(
        """1,2,3
          |""".stripMargin))
          .via(flow)
          .runWith(Sink.head)
      // #column-names
      // format: on
      future.futureValue should be(Map("eins" -> ByteString("1"), "zwei" -> ByteString("2"),
          "drei" -> ByteString("3")))
    }

  }
}
