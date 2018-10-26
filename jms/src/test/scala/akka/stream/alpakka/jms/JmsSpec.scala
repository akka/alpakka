/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.testkit.{SocketUtil, TestKit}
import javax.jms._
import org.apache.activemq.broker.BrokerService
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt}
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.mockito.MockitoSugar

abstract class JmsSpec
    extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures
    with Eventually
    with MockitoSugar {

  implicit val system = ActorSystem(this.getClass.getSimpleName)

  val decider: Supervision.Decider = ex => Supervision.Stop

  val settings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)

  implicit val materializer = ActorMaterializer(settings)

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
      s"vm://$host?create=false"
    }
    broker.setPersistent(false)
    broker.setBrokerName(host)
    broker.setUseJmx(false)
    broker.start()
    try {
      test(Context(url, broker))
      Thread.sleep(500)
    } finally {
      if (broker.isStarted) {
        broker.stop()
      }
    }
  }

  case class Context(url: String, broker: BrokerService)

  def withMockedProducer(test: ProducerMock => Unit): Unit = test(ProducerMock())

  case class ProducerMock(factory: ConnectionFactory = mock[ConnectionFactory],
                          connection: Connection = mock[Connection],
                          session: Session = mock[Session],
                          producer: MessageProducer = mock[MessageProducer]) {
    when(factory.createConnection()).thenReturn(connection)
    when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session)
    when(session.createProducer(any[javax.jms.Destination])).thenReturn(producer)
  }
}
