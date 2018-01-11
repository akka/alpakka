/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl

import java.nio.file.{Path, StandardOpenOption}
import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.javadsl
import akka.stream.javadsl.Sink
import akka.util.ByteString
import akka.japi.{function}

import scala.collection.JavaConverters._

object LogRotatorSink {
  def createFromFunction(
      f: function.Creator[function.Function[ByteString, Optional[Path]]]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    new Sink(
      akka.stream.alpakka.file.scaladsl
        .LogRotatorSink({ () =>
          val fun = f.create()
          elem =>
            Option(fun(elem).orElse(null))
        })
        .toCompletionStage()
    )

  def createFromFunctionAndOptions[T](
      f: function.Creator[function.Function[ByteString, Optional[Path]]],
      options: util.Set[StandardOpenOption]
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    new Sink(
      akka.stream.alpakka.file.scaladsl
        .LogRotatorSink({ () =>
          val fun = f.create()
          elem =>
            Option(fun(elem).orElse(null))
        }, options.asScala.toSet)
        .toCompletionStage()
    )
}
