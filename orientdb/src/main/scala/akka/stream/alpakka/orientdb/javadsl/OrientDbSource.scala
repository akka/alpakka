/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.alpakka.orientdb.impl.OrientDBSourceStage
import akka.stream.javadsl.Source
import com.orientechnologies.orient.core.record.impl.ODocument

/**
 * Java API.
 */
object OrientDbSource {

  /**
   * Read `ODocument`s from `className`.
   */
  def create(className: String, settings: OrientDbSourceSettings): Source[OrientDbReadResult[ODocument], NotUsed] =
    Source.fromGraph(
      new OrientDBSourceStage(
        className,
        Option.empty,
        settings
      )
    )

  /**
   * Read `ODocument`s from `className` or by `query`.
   */
  def create(className: String,
             settings: OrientDbSourceSettings,
             query: String): Source[OrientDbReadResult[ODocument], NotUsed] =
    Source.fromGraph(
      new OrientDBSourceStage(
        className,
        Option(query),
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
      new OrientDBSourceStage[T](
        className,
        Option(query),
        settings,
        clazz = Some(clazz)
      )
    )

}
