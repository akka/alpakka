/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import java.net.ServerSocket

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.activemq.broker.BrokerService
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

abstract class JmsSpec
    extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit =
    system.terminate()

  def withServer()(test: Context => Unit): Unit = {
    val broker = new BrokerService()
    broker.setPersistent(false)
    val host: String = "localhost"
    val serverSocket = new ServerSocket(0)
    val port = serverSocket.getLocalPort
    broker.setBrokerName(host)
    broker.setUseJmx(false)
    broker.addConnector(s"tcp://$host:$port")
    broker.start()
    try {
      test(Context(host, port, broker))
    } finally {
      if (broker.isStarted) {
        broker.stop()
      }
    }
  }

  case class Context(host: String, port: Int, broker: BrokerService)

}
