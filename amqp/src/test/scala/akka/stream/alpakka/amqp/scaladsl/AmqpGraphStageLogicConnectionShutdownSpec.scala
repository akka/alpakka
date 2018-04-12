/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.ActorMaterializer
import akka.stream.alpakka.amqp.{
  AmqpCachedConnectionProvider,
  AmqpConnectionFactoryConnectionProvider,
  AmqpProxyConnection,
  AmqpSinkSettings,
  DelegatingConnectionProvider,
  QueueDeclaration
}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.rabbitmq.client.{Connection, ConnectionFactory, ShutdownListener}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

/**
 * see [[https://github.com/akka/alpakka/issues/883]] and
 * [[https://github.com/akka/alpakka/pull/887]]
 */
class AmqpGraphStageLogicConnectionShutdownSpec extends WordSpec with Matchers with ScalaFutures {

  override implicit val patienceConfig = PatienceConfig(10.seconds)
  private implicit val executionContext = ExecutionContexts.sameThreadExecutionContext

  "registers and unregisters a single connection shutdown hook per graph" in {
    // actor system is within this test as it has to be shut down in order
    // to verify graph stage termination
    implicit val system = ActorSystem(this.getClass.getSimpleName + System.currentTimeMillis())
    implicit val materializer = ActorMaterializer()

    val spyConnectionPromise = Promise[Connection]
    val shutdownsAdded = new AtomicInteger()
    val shutdownsRemoved = new AtomicInteger()

    def spy(conn: Connection): Connection = new AmqpProxyConnection(conn) {
      override def addShutdownListener(shutdownListener: ShutdownListener): Unit = {
        shutdownsAdded.incrementAndGet()
        super.addShutdownListener(shutdownListener)
      }

      override def removeShutdownListener(shutdownListener: ShutdownListener): Unit = {
        shutdownsRemoved.incrementAndGet()
        super.removeShutdownListener(shutdownListener)
      }
    }

    val connectionFactory = new ConnectionFactory()
    val connectionProvider = DelegatingConnectionProvider(
      AmqpConnectionFactoryConnectionProvider(connectionFactory, List(("localhost", 5672))),
      conn => {
        val proxy = spy(conn)
        spyConnectionPromise.success(proxy)
        proxy
      }
    )

    val reusableConnectionProvider =
      AmqpCachedConnectionProvider(provider = connectionProvider, automaticRelease = true)

    def queueName = "amqp-conn-prov-it-spec-simple-queue-" + System.currentTimeMillis()
    val queueDeclaration = QueueDeclaration(queueName)

    val amqpSink = AmqpSink.simple(
      AmqpSinkSettings(reusableConnectionProvider)
        .withRoutingKey(queueName)
        .withDeclarations(queueDeclaration)
    )

    val input = Vector("one", "two", "three", "four")

    Future
      .traverse(input)(in => Source.single(ByteString(in)).runWith(amqpSink))
      .flatMap(_ => spyConnectionPromise.future)
      .flatMap(_ => system.terminate())
      .futureValue

    shutdownsAdded.get() should equal(input.size)
    shutdownsRemoved.get() should equal(input.size)
  }
}
