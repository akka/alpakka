/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.csv.scaladsl

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString

class CsvToMapSpec extends CsvSpec {

  def documentation(): Unit = {
    // format: off
    // #flow-type
    import akka.stream.alpakka.csv.scaladsl.CsvToMap

    val flow1: Flow[List[ByteString], Map[String, ByteString], NotUsed]
      = CsvToMap.toMap()

    val flow2: Flow[List[ByteString], Map[String, ByteString], NotUsed]
      = CsvToMap.toMap(StandardCharsets.UTF_8)

    val flow3: Flow[List[ByteString], Map[String, ByteString], NotUsed]
      = CsvToMap.withHeaders("column1", "column2", "column3")
    // #flow-type
    // format: on
  }

  "CSV to Map" should {
    "parse header line and data line into map" in {
      // #header-line
      import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}

      // #header-line
      val future =
        // format: off
      // #header-line
      Source
        .single(ByteString("""eins,zwei,drei
                             |1,2,3""".stripMargin))
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.toMap())
        .runWith(Sink.head)
      // #header-line
      // format: on
      future.futureValue should be(
        Map("eins" -> ByteString("1"), "zwei" -> ByteString("2"), "drei" -> ByteString("3"))
      )
    }

    "be OK with fewer header columns than data" in {
      val future =
        Source
          .single(ByteString("""eins,zwei
                               |1,2,3""".stripMargin))
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMap())
          .runWith(Sink.head)
      future.futureValue should be(Map("eins" -> ByteString("1"), "zwei" -> ByteString("2")))
    }

    "be OK with more header columns than data" in {
      val future =
        Source
          .single(ByteString("""eins,zwei,drei,vier
                               |1,2,3""".stripMargin))
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMap())
          .runWith(Sink.head)
      future.futureValue should be(
        Map("eins" -> ByteString("1"), "zwei" -> ByteString("2"), "drei" -> ByteString("3"))
      )
    }

    "use column names and data line into map" in {
      // #column-names
      import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}

      // #column-names
      val future =
        // format: off
      // #column-names
      Source
        .single(ByteString("""1,2,3"""))
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.withHeaders("eins", "zwei", "drei"))
        .runWith(Sink.head)
      // #column-names
      // format: on
      future.futureValue should be(
        Map("eins" -> ByteString("1"), "zwei" -> ByteString("2"), "drei" -> ByteString("3"))
      )
    }

  }
}
