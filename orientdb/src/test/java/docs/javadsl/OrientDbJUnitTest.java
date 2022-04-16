/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import docs.scaladsl.OrientDbContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
public class OrientDbJUnitTest {
  public static final OrientDbContainer CONTAINER = new OrientDbContainer();

  @BeforeClass
  public static void startContainer() {
    CONTAINER.start();
  }

  @AfterClass
  public static void stopContainer() {
    CONTAINER.stop();
  }
}
