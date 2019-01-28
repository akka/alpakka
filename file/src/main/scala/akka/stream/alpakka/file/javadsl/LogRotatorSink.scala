/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl

import java.nio.file.{Path, StandardOpenOption}
import java.util
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

object LogRotatorSink {
  def createFromFunction(
      f: function.Creator[function.Function[ByteString, Optional[Path]]]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    new Sink(
      akka.stream.alpakka.file.scaladsl
        .LogRotatorSink(asScala(f))
        .toCompletionStage()
    )

  def createFromFunctionAndOptions(
      f: function.Creator[function.Function[ByteString, Optional[Path]]],
      options: util.Set[StandardOpenOption]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    new Sink(
      akka.stream.alpakka.file.scaladsl
        .LogRotatorSink(asScala(f), options.asScala.toSet)
        .toCompletionStage()
    )

  def withSinkFactory[R](
      f: function.Creator[function.Function[ByteString, Optional[Path]]],
      target: function.Function[Path, Sink[ByteString, CompletionStage[R]]]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] = {
    val t: Path => scaladsl.Sink[ByteString, Future[R]] = path =>
      target.apply(path).asScala.mapMaterializedValue(_.toScala)
    new Sink(
      akka.stream.alpakka.file.scaladsl.LogRotatorSink
        .withSinkFactory(asScala(f), t)
        .toCompletionStage()
    )
  }

  private def asScala(
      f: function.Creator[function.Function[ByteString, Optional[Path]]]
  ): () => ByteString => Option[Path] = () => {
    val fun = f.create()
    elem =>
      Option(fun(elem).orElse(null))
  }

}
