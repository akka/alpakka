/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import java.net.ConnectException

import java.util.concurrent.atomic.AtomicInteger

import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.rabbitmq.client._

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class AmqpConnectionProvidersSpec extends AmqpSpec {

  override implicit val patienceConfig = PatienceConfig(10.seconds)
  private implicit val executionContext = ExecutionContexts.sameThreadExecutionContext

  "The AMQP Default Connection Providers" should {
    "create a new connection per invocation of LocalAmqpConnection" in {
      val connectionProvider = AmqpLocalConnectionProvider
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "not error if releasing an already closed LocalAmqpConnection" in {
      val connectionProvider = AmqpLocalConnectionProvider
      val connection1 = connectionProvider.get
      connectionProvider.release(connection1)
      connectionProvider.release(connection1)
    }

    "create a new connection per invocation of AmqpConnectionUri" in {
      val connectionProvider = AmqpUriConnectionProvider("amqp://localhost:5672")
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "not error if releasing an already closed AmqpConnectionUri" in {
      val connectionProvider = AmqpUriConnectionProvider("amqp://localhost:5672")
      val connection1 = connectionProvider.get
      connectionProvider.release(connection1)
      connectionProvider.release(connection1)
    }

    "create a new connection per invocation of AmqpConnectionDetails" in {
      val connectionProvider = AmqpDetailsConnectionProvider(List(("localhost", 5672)))
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "not error if releasing an already closed AmqpConnectionDetails" in {
      val connectionProvider = AmqpDetailsConnectionProvider(List(("localhost", 5672)))
      val connection1 = connectionProvider.get
      connectionProvider.release(connection1)
      connectionProvider.release(connection1)
    }

    "create a new connection per invocation of AmqpConnectionFactory" in {
      val connectionFactory = new ConnectionFactory()
      val connectionProvider = AmqpConnectionFactoryConnectionProvider(connectionFactory, List(("localhost", 5672)))
      val connection1 = connectionProvider.get
      val connection2 = connectionProvider.get
      connection1 should not equal connection2
      connectionProvider.release(connection1)
      connectionProvider.release(connection2)
    }

    "not error if releasing an already closed AmqpConnectionFactory" in {
      val connectionFactory = new ConnectionFactory()
      val connectionProvider = AmqpConnectionFactoryConnectionProvider(connectionFactory, List(("localhost", 5672)))
      val connection1 = connectionProvider.get
      connectionProvider.release(connection1)
      connectionProvider.release(connection1)
    }
  }

  "The AMQP Cached Connection Provider with automatic release" should {
    "reuse the same connection from LocalAmqpConnection and release it when the last client disconnects" in {
      val connectionProvider = AmqpLocalConnectionProvider
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      reusableConnectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri and release it when the last client disconnects" in {
      val connectionProvider = AmqpUriConnectionProvider("amqp://localhost:5672")
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      reusableConnectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails and release it when the last client disconnects" in {
      val connectionProvider = AmqpDetailsConnectionProvider(List(("localhost", 5672)))
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      reusableConnectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionFactory and release it when the last client disconnects" in {
      val connectionFactory = new ConnectionFactory()
      val connectionProvider = AmqpConnectionFactoryConnectionProvider(connectionFactory, List(("localhost", 5672)))
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(true)
      connection2.isOpen should be(true)
      reusableConnectionProvider.release(connection2)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "registers and unregisters a single connection shutdown hook per graph" in {
      val spyConnectionPromise = Promise[Connection]
      val shutdownsAdded = new AtomicInteger()
      val shutdownsRemoved = new AtomicInteger()

      val connectionFactory = new ConnectionFactory()
      val connectionProvider = DelegatingConnectionProvider(
        AmqpConnectionFactoryConnectionProvider(connectionFactory, List(("localhost", 5672))),
        conn => {
          val proxy = new AmqpProxyConnection(conn) {
            override def addShutdownListener(shutdownListener: ShutdownListener): Unit = {
              shutdownsAdded.incrementAndGet()
              super.addShutdownListener(shutdownListener)
            }

            override def removeShutdownListener(shutdownListener: ShutdownListener): Unit = {
              shutdownsRemoved.incrementAndGet()
              super.removeShutdownListener(shutdownListener)
            }
          }

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

      Future.traverse(input)(in => Source.single(ByteString(in)).runWith(amqpSink))
        .flatMap(_ => spyConnectionPromise.future)
        .futureValue

      shutdownsAdded.get() should equal(input.size)
      shutdownsRemoved.get() should equal(input.size)
    }
  }

  "The AMQP Cached Connection Provider without automatic release" should {
    "reuse the same connection from LocalAmqpConnection" in {
      val connectionProvider = AmqpLocalConnectionProvider
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider, automaticRelease = false)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionUri" in {
      val connectionProvider = AmqpUriConnectionProvider("amqp://localhost:5672")
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider, automaticRelease = false)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionDetails" in {
      val connectionProvider = AmqpDetailsConnectionProvider(List(("localhost", 5672)))
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider, automaticRelease = false)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "reuse the same connection from AmqpConnectionFactory" in {
      val connectionFactory = new ConnectionFactory()
      val connectionProvider = AmqpConnectionFactoryConnectionProvider(connectionFactory, List(("localhost", 5672)))
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider, automaticRelease = false)
      val connection1 = reusableConnectionProvider.get
      val connection2 = reusableConnectionProvider.get
      connection1 should equal(connection2)
      reusableConnectionProvider.release(connection1)
      connection1.isOpen should be(false)
      connection2.isOpen should be(false)
    }

    "not leave the provider in an invalid state if getting the connection fails" in {
      val connectionProvider = AmqpDetailsConnectionProvider(List(("localhost", 5673)))
      val reusableConnectionProvider = AmqpCachedConnectionProvider(connectionProvider, automaticRelease = false)
      try reusableConnectionProvider.get
      catch { case e: Throwable => e shouldBe an[ConnectException] }
      try reusableConnectionProvider.get
      catch { case e: Throwable => e shouldBe an[ConnectException] }
    }
  }
}
