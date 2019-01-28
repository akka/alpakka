/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl

import java.nio.file.{Path, StandardOpenOption}
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.javadsl
import akka.stream.scaladsl
import akka.stream.javadsl.Sink
import akka.util.ByteString
import akka.japi.function

import scala.collection.JavaConverters._
import scala.concurrent.Future

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._

object LogRotatorSink {

  /**
   * Sink directing the incoming `ByteString`s to new files whenever `generatorFunction` returns a new value.
   *
   * @param f creates a function that triggers rotation by returning a value
   */
  def createFromFunction(
      f: function.Creator[function.Function[ByteString, Optional[Path]]]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    new Sink(
      akka.stream.alpakka.file.scaladsl
        .LogRotatorSink(asScala(f))
        .toCompletionStage()
    )

  /**
   * Sink directing the incoming `ByteString`s to new files whenever `generatorFunction` returns a value.
   *
   * @param f creates a function that triggers rotation by returning a value
   * @param fileOpenOptions file options for file creation
   */
  def createFromFunctionAndOptions(
      f: function.Creator[function.Function[ByteString, Optional[Path]]],
      fileOpenOptions: java.util.Set[StandardOpenOption]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    new Sink(
      akka.stream.alpakka.file.scaladsl
        .LogRotatorSink(asScala(f), fileOpenOptions.asScala.toSet)
        .toCompletionStage()
    )

  def withSinkFactory[C, R](
      f: function.Creator[function.Function[ByteString, Optional[C]]],
      target: function.Function[C, Sink[ByteString, CompletionStage[R]]]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] = {
    val t: C => scaladsl.Sink[ByteString, Future[R]] = path =>
      target.apply(path).asScala.mapMaterializedValue(_.toScala)
    new Sink(
      akka.stream.alpakka.file.scaladsl.LogRotatorSink
        .withSinkFactory(asScala[C](f), t)
        .toCompletionStage()
    )
  }

  private def asScala[C](
      f: function.Creator[function.Function[ByteString, Optional[C]]]
  ): () => ByteString => Option[C] = () => {
    val fun = f.create()
    elem =>
      fun(elem).asScala
  }

}
