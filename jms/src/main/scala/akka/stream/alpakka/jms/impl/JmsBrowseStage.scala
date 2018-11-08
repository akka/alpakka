/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.{Enumeration => JEnumeration}

import akka.annotation.InternalApi
import akka.stream.alpakka.jms.{AcknowledgeMode, Destination, JmsBrowseSettings}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import javax.jms._

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsBrowseStage(settings: JmsBrowseSettings, queue: Destination)
    extends GraphStage[SourceShape[Message]] {
  private val out = Outlet[Message]("JmsBrowseStage.out")
  val shape = SourceShape(out)

  override protected def initialAttributes: Attributes =
    ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      setHandler(out, this)

      var connection: Connection = _
      var session: Session = _
      var browser: QueueBrowser = _
      var messages: JEnumeration[Message] = _

      override def preStart(): Unit = {
        val ackMode = settings.acknowledgeMode.getOrElse(AcknowledgeMode.AutoAcknowledge).mode
        connection = settings.connectionFactory.createConnection()
        connection.start()

        session = connection.createSession(false, ackMode)
        browser = session.createBrowser(session.createQueue(queue.name), settings.selector.orNull)
        messages = browser.getEnumeration.asInstanceOf[JEnumeration[Message]]
      }

      override def postStop(): Unit = {
        messages = null
        if (browser ne null) {
          browser.close()
          browser = null
        }
        if (session ne null) {
          session.close()
          session = null
        }
        if (connection ne null) {
          connection.close()
          connection = null
        }
      }

      def onPull(): Unit =
        if (messages.hasMoreElements) {
          push(out, messages.nextElement())
        } else {
          complete(out)
        }
    }
}
