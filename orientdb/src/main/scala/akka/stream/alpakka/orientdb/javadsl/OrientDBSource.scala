/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.javadsl.Source
import com.orientechnologies.orient.core.record.impl.ODocument

object OrientDBSource {

  /**
   * Java API: creates a [[OrientDBSourceStage]] that producer as ODocument
   */
  def create(className: String,
             settings: OrientDBSourceSettings,
             query: String): Source[OOutgoingMessage[ODocument], NotUsed] =
    Source.fromGraph(
      new OrientDBSourceStage(
        className,
        Option(query),
        settings,
        new ODocumentMessageReader[ODocument]()
      )
    )

  /**
   * Java API: creates a [[OrientDBSourceStage]] that produces as specific type
   */
  def typed[T](className: String,
               settings: OrientDBSourceSettings,
               clazz: Class[T],
               query: String = null): Source[OOutgoingMessage[T], NotUsed] =
    Source.fromGraph(
      new OrientDBSourceStage[T](
        className,
        Option(query),
        settings,
        new ODocumentMessageReader[T](),
        clazz = Some(clazz)
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
