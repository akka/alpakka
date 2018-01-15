package akka.stream.alpakka.parquet.javasql

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.javadsl.Sink
import akka.stream.alpakka.parquet.scaladsl.{Parquet => ScalaParquet}

object Parquet {
  def plainSink[T](fileName: String, recordClass: Class[T]): Sink[T, CompletionStage[Done]] =
    ScalaParquet()
}
