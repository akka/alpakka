/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import com.dimafeng.testcontainers.MongoDBContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import scala.Option;

@RunWith(Suite.class)
public class MongoJUnitTest {
  public static final MongoDBContainer CONTAINER = new MongoDBContainer(Option.empty());

  @BeforeClass
  public static void startContainer() {
    CONTAINER.start();
  }

  @AfterClass
  public static void stopContainer() {
    CONTAINER.stop();
  }

  String getConnectionString() {
    return "mongodb://"
        + CONTAINER.container().getContainerIpAddress()
        + ":"
        + CONTAINER.container().getMappedPort(27017);
  }
}
