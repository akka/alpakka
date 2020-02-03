/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.alpakka.orientdb.impl.OrientDbSourceStage
import akka.stream.scaladsl.Source
import com.orientechnologies.orient.core.record.impl.ODocument

/**
 * Scala API.
 */
object OrientDbSource {

  /**
   * Read `ODocument`s from `className` or by `query`.
   */
  def apply(className: String,
            settings: OrientDbSourceSettings,
            query: Option[String] = None): Source[OrientDbReadResult[ODocument], NotUsed] =
    Source.fromGraph(
      new OrientDbSourceStage(
        className,
        query,
        settings
      )
    )

  /**
   * Read elements of `T` from `className` or by `query`.
   */
  def typed[T](className: String,
               settings: OrientDbSourceSettings,
               clazz: Class[T],
               query: String = null): Source[OrientDbReadResult[T], NotUsed] =
    Source.fromGraph(
      new OrientDbSourceStage[T](
        className,
        Option(query),
        settings,
        clazz = Some(clazz)
      )
    )

}
