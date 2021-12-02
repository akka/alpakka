/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import com.rabbitmq.client.{Address, Channel, ConfirmCallback, ConfirmListener, Connection, ConnectionFactory}
import org.mockito.ArgumentMatchers._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito.when

trait AmqpMocking extends MockitoSugar {

  val channelMock: Channel = mock[Channel]

  val connectionMock: Connection = mock[Connection]

  def connectionFactoryMock: ConnectionFactory = {
    val connectionFactory = mock[ConnectionFactory]

    when(connectionFactory.newConnection(any[java.util.List[Address]]))
      .thenReturn(connectionMock)

    when(connectionMock.createChannel())
      .thenReturn(channelMock)

    when(channelMock.addConfirmListener(any[ConfirmCallback](), any[ConfirmCallback]()))
      .thenReturn(mock[ConfirmListener])

    connectionFactory
  }
}
