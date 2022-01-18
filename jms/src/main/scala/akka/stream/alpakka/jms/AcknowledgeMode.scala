/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms

/**
 * JMS acknowledge modes.
 * See [[javax.jms.Connection#createSession-boolean-int-]]
 */
final class AcknowledgeMode(val mode: Int) {
  override def equals(other: Any): Boolean = other match {
    case that: AcknowledgeMode => this.mode == that.mode
    case _ => false
  }
  override def hashCode: Int = mode
  override def toString: String = s"AcknowledgeMode(${AcknowledgeMode.asString(this)})"
}

object AcknowledgeMode {
  val AutoAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.AUTO_ACKNOWLEDGE)
  val ClientAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.CLIENT_ACKNOWLEDGE)
  val DupsOkAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.DUPS_OK_ACKNOWLEDGE)
  val SessionTransacted: AcknowledgeMode = new AcknowledgeMode(jms.Session.SESSION_TRANSACTED)

  /**
   * Interpret string to corresponding acknowledge mode.
   */
  def from(s: String): AcknowledgeMode = s match {
    case "auto" => AutoAcknowledge
    case "client" => ClientAcknowledge
    case "duplicates-ok" => DupsOkAcknowledge
    case "session" => SessionTransacted
    case other =>
      try {
        val mode = other.toInt
        new AcknowledgeMode(mode)
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(
            s"can't read AcknowledgeMode '$other', (known are auto, client, duplicates-ok, session, or an integer value)"
          )
      }
  }

  /**
   * Convert to a string presentation.
   */
  def asString(mode: AcknowledgeMode): String = mode match {
    case AutoAcknowledge => "auto"
    case ClientAcknowledge => "client"
    case DupsOkAcknowledge => "duplicates-ok"
    case SessionTransacted => "session"
    case other => other.mode.toString
  }

}
