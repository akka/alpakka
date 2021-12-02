/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.jms.{Destination, JmsBrowseSettings}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import javax.jms

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsBrowseStage(settings: JmsBrowseSettings, queue: Destination)
    extends GraphStage[SourceShape[jms.Message]] {
  private val out = Outlet[jms.Message]("JmsBrowseStage.out")
  val shape = SourceShape(out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name("JmsBrowse") and ActorAttributes.IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      setHandler(out, this)

      var connection: jms.Connection = _
      var session: jms.Session = _
      var browser: jms.QueueBrowser = _
      var messages: java.util.Enumeration[jms.Message] = _

      override def preStart(): Unit = {
        val ackMode = settings.acknowledgeMode.mode
        connection = settings.connectionFactory.createConnection()
        connection.start()

        session = connection.createSession(false, ackMode)
        browser = session.createBrowser(session.createQueue(queue.name), settings.selector.orNull)
        messages = browser.getEnumeration.asInstanceOf[java.util.Enumeration[jms.Message]]
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
