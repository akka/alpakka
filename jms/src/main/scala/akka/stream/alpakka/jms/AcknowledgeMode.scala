/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms

final class AcknowledgeMode(val mode: Int) extends AnyVal

object AcknowledgeMode {
  val AutoAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.AUTO_ACKNOWLEDGE)
  val ClientAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.CLIENT_ACKNOWLEDGE)
  val DupsOkAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.DUPS_OK_ACKNOWLEDGE)
  val SessionTransacted: AcknowledgeMode = new AcknowledgeMode(jms.Session.SESSION_TRANSACTED)

  def from(s: String): AcknowledgeMode = s match {
    case "auto" => AutoAcknowledge
    case "client" => ClientAcknowledge
    case "duplicates-ok" => DupsOkAcknowledge
    case "session" => SessionTransacted
    case other =>
      throw new IllegalArgumentException(
        s"can't read AcknowledgeMode '$other', (known are auto, client, duplicates-ok, session)"
      )
  }

  def asString(mode: AcknowledgeMode) = mode match {
    case AutoAcknowledge => "auto"
    case ClientAcknowledge => "client"
    case DupsOkAcknowledge => "duplicates-ok"
    case SessionTransacted => "session"
    case other =>
      throw new IllegalArgumentException(s"don't know AcknowledgeMode '$other'")
  }

}
