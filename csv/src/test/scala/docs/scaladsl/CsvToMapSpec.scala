/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.ByteString

class CsvToMapSpec extends CsvSpec {

  def documentation(): Unit = {
    // format: off
    // #flow-type
    import akka.stream.alpakka.csv.scaladsl.CsvToMap

    // keep values as ByteString
    val flow1: Flow[List[ByteString], Map[String, ByteString], NotUsed]
      = CsvToMap.toMap()

    val flow2: Flow[List[ByteString], Map[String, ByteString], NotUsed]
      = CsvToMap.toMap(StandardCharsets.UTF_8)

    val flow3: Flow[List[ByteString], Map[String, ByteString], NotUsed]
      = CsvToMap.withHeaders("column1", "column2", "column3")

    // values as String (decode ByteString)
    val flow4: Flow[List[ByteString], Map[String, String], NotUsed]
    = CsvToMap.toMapAsStrings(StandardCharsets.UTF_8)

    val flow5: Flow[List[ByteString], Map[String, String], NotUsed]
    = CsvToMap.withHeadersAsStrings(StandardCharsets.UTF_8, "column1", "column2", "column3")
    // #flow-type
    // format: on

    Source.single(List(ByteString("a"), ByteString("b"))).via(flow1).runWith(Sink.ignore)
    Source.single(List(ByteString("a"), ByteString("b"))).via(flow2).runWith(Sink.ignore)
    Source.single(List(ByteString("a"), ByteString("b"))).via(flow3).runWith(Sink.ignore)
    Source.single(List(ByteString("a"), ByteString("b"))).via(flow4).runWith(Sink.ignore)
    Source.single(List(ByteString("a"), ByteString("b"))).via(flow5).runWith(Sink.ignore)
  }

  "CSV to Map" should {
    "parse header line and data line into map" in assertAllStagesStopped {
      // #header-line
      import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}

      // #header-line
      val future =
        // format: off
      // #header-line
      // values as ByteString
      Source
        .single(ByteString("""eins,zwei,drei
                             |11,12,13
                             |21,22,23
                             |""".stripMargin))
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.toMap())
        .runWith(Sink.seq)
      // #header-line
      // format: on
      val result = future.futureValue
      // #header-line

      result should be(
        Seq(
          Map("eins" -> ByteString("11"), "zwei" -> ByteString("12"), "drei" -> ByteString("13")),
          Map("eins" -> ByteString("21"), "zwei" -> ByteString("22"), "drei" -> ByteString("23"))
        )
      )
      // #header-line
    }

    "be OK with fewer header columns than data" in assertAllStagesStopped {
      val future =
        Source
          .single(ByteString("""eins,zwei
                               |1,2,3
                               |""".stripMargin))
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMap())
          .runWith(Sink.head)
      future.futureValue should be(Map("eins" -> ByteString("1"), "zwei" -> ByteString("2")))
    }

    "be OK with more header columns than data" in assertAllStagesStopped {
      val future =
        Source
          .single(ByteString("""eins,zwei,drei,vier
                               |1,2,3
                               |""".stripMargin))
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMap())
          .runWith(Sink.head)
      future.futureValue should be(
        Map("eins" -> ByteString("1"), "zwei" -> ByteString("2"), "drei" -> ByteString("3"))
      )
    }

    "parse header line and decode data line" in assertAllStagesStopped {
      val future =
        // format: off
      // #header-line

      // values as String
      Source
        .single(ByteString("""eins,zwei,drei
                             |11,12,13
                             |21,22,23
                             |""".stripMargin))
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.toMapAsStrings())
        .runWith(Sink.seq)
      // #header-line
      // format: on
      val result = future.futureValue
      // #header-line

      result should be(
        Seq(
          Map("eins" -> "11", "zwei" -> "12", "drei" -> "13"),
          Map("eins" -> "21", "zwei" -> "22", "drei" -> "23")
        )
      )
      // #header-line
    }

    "use column names and data line into map" in assertAllStagesStopped {
      // #column-names
      import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}

      // #column-names
      val future =
        // format: off
      // #column-names
      // values as ByteString
      Source
        .single(ByteString(
          """11,12,13
            |21,22,23
            |""".stripMargin))
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.withHeaders("eins", "zwei", "drei"))
        .runWith(Sink.seq)
      // #column-names
      // format: on
      val result = future.futureValue
      // #column-names

      result should be(
        Seq(
          Map("eins" -> ByteString("11"), "zwei" -> ByteString("12"), "drei" -> ByteString("13")),
          Map("eins" -> ByteString("21"), "zwei" -> ByteString("22"), "drei" -> ByteString("23"))
        )
      )
      // #column-names
    }

    "use column names and decode data line into map" in assertAllStagesStopped {
      val future =
        // format: off
      // #column-names

      // values as String
      Source
        .single(ByteString("""11,12,13
                             |21,22,23
                             |""".stripMargin))
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.withHeadersAsStrings(StandardCharsets.UTF_8, "eins", "zwei", "drei"))
        .runWith(Sink.seq)
      // #column-names
      // format: on
      val result = future.futureValue
      // #column-names

      result should be(
        Seq(
          Map("eins" -> "11", "zwei" -> "12", "drei" -> "13"),
          Map("eins" -> "21", "zwei" -> "22", "drei" -> "23")
        )
      )
      // #column-names
    }

  }
}
