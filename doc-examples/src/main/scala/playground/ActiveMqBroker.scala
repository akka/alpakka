/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package playground

import javax.jms.ConnectionFactory

import akka.Done
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService

import scala.concurrent.{ExecutionContext, Future}

/**
 * To start an ActiveMQ broker be sure to include these dependencies:
 *
 *  "javax.jms" % "jms" % "1.1",
 *  "org.apache.activemq" % "activemq-all" % "5.14.4"
 */
class ActiveMqBroker {

  var brokerService: Option[BrokerService] = None

  def start(): BrokerService = {
    val broker = new BrokerService()
    broker.setBrokerName("localhost")
    broker.setUseJmx(false)
    broker.start()
    brokerService = Some(broker)
    broker
  }

  def stop()(implicit ec: ExecutionContext): Future[Done] =
    brokerService.fold(Future.successful(Done)) { broker =>
      Future {
        broker.stop()
        scala.concurrent.blocking {
          broker.waitUntilStopped()
        }
        Done
      }
    }

  def createConnectionFactory: ConnectionFactory = new ActiveMQConnectionFactory("vm://localhost?create=false")

}

object ActiveMqBroker extends ActiveMqBroker
