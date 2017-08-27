/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.apache.activemq.broker.BrokerService
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import akka.testkit.SocketUtil

abstract class JmsSpec
    extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def withServer(network: Boolean = true)(test: Context => Unit): Unit = {
    val broker = new BrokerService()
    val host: String = "localhost"
    val url = if (network) {
      val port = SocketUtil.temporaryServerAddress(host).getPort
      val serverUrl = s"tcp://$host:$port"
      broker.addConnector(serverUrl)
      serverUrl
    } else {
      s"vm://$host"
    }
    broker.setPersistent(false)
    broker.setBrokerName(host)
    broker.setUseJmx(false)
    broker.start()
    try {
      test(Context(url, broker))
    } finally {
      if (broker.isStarted) {
        broker.stop()
      }
    }
  }

  case class Context(url: String, broker: BrokerService)

}
