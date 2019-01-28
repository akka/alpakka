/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.alpakka.orientdb.impl.{OrientDBSourceStage}
import akka.stream.javadsl.Source
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.immutable

object OrientDbSource {

  /**
   * Java API: creates a [[OrientDBSourceStage]] that producer as ODocument
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
   * Java API: creates a [[OrientDBSourceStage]] that produces as specific type
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
