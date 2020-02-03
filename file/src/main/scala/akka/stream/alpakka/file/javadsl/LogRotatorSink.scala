/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
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

/**
 * Java API.
 */
object LogRotatorSink {

  /**
   * Sink directing the incoming `ByteString`s to new files whenever `triggerGenerator` returns a value.
   *
   * @param triggerGeneratorCreator creates a function that triggers rotation by returning a value
   */
  def createFromFunction(
      triggerGeneratorCreator: function.Creator[function.Function[ByteString, Optional[Path]]]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    new Sink(
      akka.stream.alpakka.file.scaladsl
        .LogRotatorSink(asScala(triggerGeneratorCreator))
        .toCompletionStage()
    )

  /**
   * Sink directing the incoming `ByteString`s to new files whenever `triggerGenerator` returns a value.
   *
   * @param triggerGeneratorCreator creates a function that triggers rotation by returning a value
   * @param fileOpenOptions file options for file creation
   */
  def createFromFunctionAndOptions(
      triggerGeneratorCreator: function.Creator[function.Function[ByteString, Optional[Path]]],
      fileOpenOptions: java.util.Set[StandardOpenOption]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    new Sink(
      akka.stream.alpakka.file.scaladsl
        .LogRotatorSink(asScala(triggerGeneratorCreator), fileOpenOptions.asScala.toSet)
        .toCompletionStage()
    )

  /**
   * Sink directing the incoming `ByteString`s to a new `Sink` created by `sinkFactory` whenever `triggerGenerator` returns a value.
   *
   * @param triggerGeneratorCreator creates a function that triggers rotation by returning a value
   * @param sinkFactory creates sinks for `ByteString`s from the value returned by `triggerGenerator`
   * @tparam C criterion type (for files a `Path`)
   * @tparam R result type in materialized futures of `sinkFactory`
    **/
  def withSinkFactory[C, R](
      triggerGeneratorCreator: function.Creator[function.Function[ByteString, Optional[C]]],
      sinkFactory: function.Function[C, Sink[ByteString, CompletionStage[R]]]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] = {
    val t: C => scaladsl.Sink[ByteString, Future[R]] = path =>
      sinkFactory.apply(path).asScala.mapMaterializedValue(_.toScala)
    new Sink(
      akka.stream.alpakka.file.scaladsl.LogRotatorSink
        .withSinkFactory(asScala[C](triggerGeneratorCreator), t)
        .toCompletionStage()
    )
  }

  private def asScala[C](
      f: function.Creator[function.Function[ByteString, Optional[C]]]
  ): () => ByteString => Option[C] = () => {
    val fun = f.create()
    elem => fun(elem).asScala
  }

}
