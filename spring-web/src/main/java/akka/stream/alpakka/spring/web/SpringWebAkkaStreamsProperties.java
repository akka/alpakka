/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.spring.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "akka.stream.alpakka.spring.web")
public class SpringWebAkkaStreamsProperties {

  private String actorSystemName;

  public String getActorSystemName() {
    return actorSystemName;
  }

  public void setActorSystemName(String actorSystemName) {
    this.actorSystemName = actorSystemName;
  }
}
