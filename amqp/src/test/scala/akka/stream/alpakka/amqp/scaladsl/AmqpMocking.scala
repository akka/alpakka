/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import com.rabbitmq.client.{Address, Channel, ConfirmCallback, ConfirmListener, Connection, ConnectionFactory}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.mock
import org.mockito.Mockito.when

trait AmqpMocking {

  val channelMock: Channel = mock(classOf[Channel])

  val connectionMock: Connection = mock(classOf[Connection])

  def connectionFactoryMock: ConnectionFactory = {
    val connectionFactory = mock(classOf[ConnectionFactory])

    when(connectionFactory.newConnection(any[java.util.List[Address]]))
      .thenReturn(connectionMock)

    when(connectionMock.createChannel())
      .thenReturn(channelMock)

    when(channelMock.addConfirmListener(any[ConfirmCallback](), any[ConfirmCallback]()))
      .thenReturn(mock(classOf[ConfirmListener]))

    connectionFactory
  }
}
