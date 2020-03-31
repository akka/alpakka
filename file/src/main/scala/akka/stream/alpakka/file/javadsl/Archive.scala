/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl

import akka.NotUsed
import akka.stream.alpakka.file.{scaladsl, ArchiveMetadata, TarArchiveMetadata}
import akka.stream.javadsl.Flow
import akka.util.ByteString
import akka.japi.Pair
import akka.stream.javadsl.Source

/**
 * Java API.
 */
object Archive {

  /**
   * Flow for compressing multiple files into one ZIP file.
   */
  def zip(): Flow[Pair[ArchiveMetadata, Source[ByteString, NotUsed]], ByteString, NotUsed] =
    Flow
      .create[Pair[ArchiveMetadata, Source[ByteString, NotUsed]]]()
      .map(func(pair => (pair.first, pair.second.asScala)))
      .via(scaladsl.Archive.zip().asJava)

  /**
   * Flow for packaging multiple files into one TAR file.
   */
  def tar(): Flow[Pair[TarArchiveMetadata, Source[ByteString, NotUsed]], ByteString, NotUsed] =
    Flow
      .create[Pair[TarArchiveMetadata, Source[ByteString, NotUsed]]]()
      .map(func(pair => (pair.first, pair.second.asScala)))
      .via(scaladsl.Archive.tar().asJava)

  private def func[T, R](f: T => R) = new akka.japi.function.Function[T, R] {
    override def apply(param: T): R = f(param)
  }
}
