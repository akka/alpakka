/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.persistence.writer

import akka.actor.ExtendedActorSystem
import akka.persistence.journal.{ EventAdapter, EventSeq }

class EventAdapterB(system: ExtendedActorSystem) extends EventAdapter {
  override def manifest(event: Any): String = ""

  override def fromJournal(event: Any, manifest: String): EventSeq =
    EventSeq(event)

  override def toJournal(event: Any): Any = event match {
    case MyMessage(str) => MyMessage(s"$str - b")
  }
}
