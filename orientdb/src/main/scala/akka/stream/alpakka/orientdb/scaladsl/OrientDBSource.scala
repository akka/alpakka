/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.alpakka.orientdb.impl.{OrientDBSourceStage}
import akka.stream.scaladsl.Source
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.immutable

object OrientDBSource {

  /**
   * Scala API: creates a [[OrientDBSourceStage]] that produces as ODocument
   */
  def apply(className: String,
            settings: OrientDBSourceSettings,
            query: Option[String] = None): Source[OrientDbReadResult[ODocument], NotUsed] =
    Source.fromGraph(
      new OrientDBSourceStage(
        className,
        query,
        settings
      )
    )

}
