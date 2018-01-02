/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl;

import static org.junit.Assert.*;

import akka.stream.alpakka.amqp.*;
import com.rabbitmq.client.Connection;
import org.junit.Test;

public class AmqpConnectionSettingsTest {

  @Test
  public void DefaultAmqpConnectionCreatesNewConnection() throws Exception {
    AmqpConnectionSettings connectionSettings = DefaultAmqpConnection.getInstance();
    AmqpConnectionProvider connectionProvider = DefaultAmqpConnectionProvider.create(connectionSettings);
    Connection connection1 = DefaultAmqpConnection.getConnection();
    Connection connection2 = DefaultAmqpConnection.getConnection();
    assertNotEquals(connection1, connection2);
    connectionProvider.release(connection1);
    connectionProvider.release(connection2);
  }

  @Test
  public void LocalAmqpConnectionCreatesNewConnection() throws Exception {
    AmqpConnectionSettings connectionSettings = AmqpConnectionLocal.getInstance();
    AmqpConnectionProvider connectionProvider = DefaultAmqpConnectionProvider.create(connectionSettings);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertNotEquals(connection1, connection2);
    connectionProvider.release(connection1);
    connectionProvider.release(connection2);
  }

  @Test
  public void AmqpConnectionUriCreatesNewConnection() throws Exception {
    AmqpConnectionUri connectionSettings = AmqpConnectionUri.create("amqp://localhost:5672");
    AmqpConnectionProvider connectionProvider = DefaultAmqpConnectionProvider.create(connectionSettings);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertNotEquals(connection1, connection2);
    connectionProvider.release(connection1);
    connectionProvider.release(connection2);
  }

  @Test
  public void AmqpConnectionDetailsCreatesNewConnection() throws Exception {
    AmqpConnectionDetails connectionSettings = AmqpConnectionDetails.create("localhost", 5672);
    AmqpConnectionProvider connectionProvider = DefaultAmqpConnectionProvider.create(connectionSettings);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertNotEquals(connection1, connection2);
    connectionProvider.release(connection1);
    connectionProvider.release(connection2);
  }



  @Test
  public void ReusableAMQPConnectionProviderWithAutomaticReleaseAndLocalAmqpConnectionReusesConnection() throws Exception {
    AmqpConnectionSettings connectionSettings = AmqpConnectionLocal.getInstance();
    AmqpConnectionProvider connectionProvider = ReusableAmqpConnectionProvider.create(connectionSettings);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertEquals(connection1, connection2);
    connectionProvider.release(connection1);
    assertTrue(connection1.isOpen());
    assertTrue(connection2.isOpen());
    connectionProvider.release(connection2);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableAMQPConnectionProviderWithAutomaticReleaseAndAmqpConnectionUriReusesConnection() throws Exception {
    AmqpConnectionSettings connectionSettings = AmqpConnectionUri.create("amqp://localhost:5672");
    AmqpConnectionProvider connectionProvider = ReusableAmqpConnectionProvider.create(connectionSettings);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertEquals(connection1, connection2);
    connectionProvider.release(connection1);
    assertTrue(connection1.isOpen());
    assertTrue(connection2.isOpen());
    connectionProvider.release(connection2);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableAMQPConnectionProviderWithAutomaticReleaseAndAmqpConnectionDetailsReusesConnection() throws Exception {
    AmqpConnectionSettings connectionSettings = AmqpConnectionDetails.create("localhost", 5672);
    AmqpConnectionProvider connectionProvider = ReusableAmqpConnectionProvider.create(connectionSettings);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertEquals(connection1, connection2);
    connectionProvider.release(connection1);
    assertTrue(connection1.isOpen());
    assertTrue(connection2.isOpen());
    connectionProvider.release(connection2);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableAMQPConnectionProviderWithoutAutomaticReleaseAndLocalAmqpConnectionReusesConnection() throws Exception {
    AmqpConnectionSettings connectionSettings = AmqpConnectionLocal.getInstance();
    AmqpConnectionProvider connectionProvider = ReusableAmqpConnectionProvider.create(connectionSettings, false);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertEquals(connection1, connection2);
    connectionProvider.release(connection1);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableAMQPConnectionProviderWithoutAutomaticReleaseAndAmqpConnectionUriReusesConnection() throws Exception {
    AmqpConnectionSettings connectionSettings = AmqpConnectionUri.create("amqp://localhost:5672");
    AmqpConnectionProvider connectionProvider = ReusableAmqpConnectionProvider.create(connectionSettings, false);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertEquals(connection1, connection2);
    connectionProvider.release(connection1);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableAMQPConnectionProviderWithoutAutomaticReleaseAndAmqpConnectionDetailsReusesConnection() throws Exception {
    AmqpConnectionSettings connectionSettings = AmqpConnectionDetails.create("localhost", 5672);
    AmqpConnectionProvider connectionProvider = ReusableAmqpConnectionProvider.create(connectionSettings, false);
    Connection connection1 = connectionProvider.get();
    Connection connection2 = connectionProvider.get();
    assertEquals(connection1, connection2);
    connectionProvider.release(connection1);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }
}
