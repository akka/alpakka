package akka.stream.alpakka.csv.scaladsl

import java.nio.charset.{Charset, StandardCharsets}

import akka.NotUsed
import akka.stream.alpakka.csv.CsvToMapStage
import akka.stream.scaladsl.Flow
import akka.util.ByteString

object CsvToMap {

  def toMap(charset: Charset = StandardCharsets.UTF_8): Flow[List[ByteString], Map[String, ByteString], NotUsed] =
    Flow[List[ByteString]].via(new CsvToMapStage(columnNames = None, charset)).named("csvToMap")

  def withHeaders(columnNames: String*): Flow[List[ByteString], Map[String, ByteString], NotUsed] =
    Flow[List[ByteString]].via(new CsvToMapStage(Some(columnNames.toList), StandardCharsets.UTF_8)).named("csvToMap")
}
