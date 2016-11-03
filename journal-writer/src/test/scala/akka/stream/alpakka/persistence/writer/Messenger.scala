/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.persistence.writer

import akka.persistence.PersistentActor

class Messenger(val persistenceId: String, override val journalPluginId: String = "") extends PersistentActor {
  var events: Seq[Any] = Seq.empty[Any]
  override def receiveRecover: Receive = {
    case event: MyMessage => events :+= event
    case event            =>
  }

  override def receiveCommand: Receive = {
    case "state" =>
      sender() ! events
    case str: String => persist(MyMessage(str)) { _ =>
      sender() ! "ack"
    }
  }
}
