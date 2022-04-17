/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.stream.alpakka.dynamodb.DynamoDbLocalContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
public class DynamoDbJUnitTest {
  public static final DynamoDbLocalContainer CONTAINER = new DynamoDbLocalContainer();

  @BeforeClass
  public static void startContainer() {
    CONTAINER.start();
  }

  @AfterClass
  public static void stopContainer() {
    CONTAINER.stop();
  }
}
