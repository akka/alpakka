/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
