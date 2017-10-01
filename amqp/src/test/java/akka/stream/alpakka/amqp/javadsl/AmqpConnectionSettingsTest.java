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
    Connection connection1 = DefaultAmqpConnection.getConnection();
    Connection connection2 = DefaultAmqpConnection.getConnection();
    assertNotEquals(connection1, connection2);
    connection1.close();
    connection2.close();
  }

  @Test
  public void LocalAmqpConnectionCreatesNewConnection() throws Exception {
    AmqpConnectionLocal connectionSettings = new AmqpConnectionLocal();
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertNotEquals(connection1, connection2);
    connection1.close();
    connection2.close();
  }

  @Test
  public void AmqpConnectionUriCreatesNewConnection() throws Exception {
    AmqpConnectionUri connectionSettings = new AmqpConnectionUri("amqp://localhost:5672");
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertNotEquals(connection1, connection2);
    connection1.close();
    connection2.close();
  }

  @Test
  public void AmqpConnectionDetailsCreatesNewConnection() throws Exception {
    //#connection-settings-declaration
    AmqpConnectionDetails connectionSettings = AmqpConnectionDetails.create("localhost", 5672);
    //#connection-settings-declaration
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertNotEquals(connection1, connection2);
    connection1.close();
    connection2.close();
  }

  @Test
  public void ReusableLocalAmqpConnectionReusesConnection() throws Exception {
    ReusableAmqpConnectionSettings connectionSettings = new ReusableAmqpConnectionSettings(new AmqpConnectionLocal());
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection();
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableAmqpConnectionUriReusesConnection() throws Exception {
    ReusableAmqpConnectionSettings connectionSettings = new ReusableAmqpConnectionSettings(new AmqpConnectionUri("amqp://localhost:5672"));
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection();
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableAmqpConnectionDetailsReusesConnection() throws Exception {
    //#reusable-connection-settings-declaration
    ReusableAmqpConnectionSettings connectionSettings = new ReusableAmqpConnectionSettings(AmqpConnectionDetails.create("localhost", 5672));
    //#reusable-connection-settings-declaration
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection();
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableWithAutomaticReleaseLocalAmqpConnectionReusesConnection() throws Exception {
    ReusableAmqpConnectionSettingsWithAutomaticRelease connectionSettings = new ReusableAmqpConnectionSettingsWithAutomaticRelease(new AmqpConnectionLocal());
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection(false);
    assertTrue(connection1.isOpen());
    assertTrue(connection2.isOpen());
    connectionSettings.releaseConnection(false);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableWithAutomaticReleaseAmqpConnectionUriReusesConnection() throws Exception {
    ReusableAmqpConnectionSettingsWithAutomaticRelease connectionSettings = new ReusableAmqpConnectionSettingsWithAutomaticRelease(new AmqpConnectionUri("amqp://localhost:5672"));
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection(false);
    assertTrue(connection1.isOpen());
    assertTrue(connection2.isOpen());
    connectionSettings.releaseConnection(false);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableWithAutomaticReleaseAmqpConnectionDetailsReusesConnection() throws Exception {
    //#reusable-connection-settings-with-automatic-release-declaration
    ReusableAmqpConnectionSettingsWithAutomaticRelease connectionSettings = new ReusableAmqpConnectionSettingsWithAutomaticRelease(AmqpConnectionDetails.create("localhost", 5672));
    //#reusable-connection-settings-with-automatic-release-declaration
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection(false);
    assertTrue(connection1.isOpen());
    assertTrue(connection2.isOpen());
    connectionSettings.releaseConnection(false);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableWithAutomaticReleaseLocalAmqpConnectionReusesConnectionAndReleaseItIfForced() throws Exception {
    ReusableAmqpConnectionSettingsWithAutomaticRelease connectionSettings = new ReusableAmqpConnectionSettingsWithAutomaticRelease(new AmqpConnectionLocal());
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection(true);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableWithAutomaticReleaseAmqpConnectionUriReusesConnectionAndReleaseItIfForced() throws Exception {
    ReusableAmqpConnectionSettingsWithAutomaticRelease connectionSettings = new ReusableAmqpConnectionSettingsWithAutomaticRelease(new AmqpConnectionUri("amqp://localhost:5672"));
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection(true);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }

  @Test
  public void ReusableWithAutomaticReleaseAmqpConnectionDetailsReusesConnectionAndReleaseItIfForced() throws Exception {
    ReusableAmqpConnectionSettingsWithAutomaticRelease connectionSettings = new ReusableAmqpConnectionSettingsWithAutomaticRelease(AmqpConnectionDetails.create("localhost", 5672));
    Connection connection1 = connectionSettings.getConnection();
    Connection connection2 = connectionSettings.getConnection();
    assertEquals(connection1, connection2);
    connectionSettings.releaseConnection(true);
    assertFalse(connection1.isOpen());
    assertFalse(connection2.isOpen());
  }
}
