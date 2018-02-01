/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.scaladsl.Source
import com.orientechnologies.orient.core.record.impl.ODocument

object OrientDBSource {

  /**
   * Scala API: creates a [[OrientDBSourceStage]] that produces as ODocument
   */
  def apply(className: String,
            settings: OrientDBSourceSettings,
            query: Option[String] = None): Source[OOutgoingMessage[ODocument], NotUsed] =
    Source.fromGraph(
      new OrientDBSourceStage(
        className,
        query,
        settings,
        new ODocumentMessageReader[ODocument]()
      )
    )

  private class ODocumentMessageReader[T] extends MessageReader[T] {

    override def convert(oDocs: List[T]): OSQLResponse[T] =
      try {
        OSQLResponse(None, Some(OSQLResult(oDocs.map(OOutgoingMessage(_)))))
      } catch {
        case exception: Exception => OSQLResponse(Some(exception.toString), None)
      }
  }
}
