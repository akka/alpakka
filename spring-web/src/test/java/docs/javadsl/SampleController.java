/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #use
import javax.annotation.PostConstruct;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.LoggingAdapter;
import akka.stream.javadsl.Source;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.util.Assert;

@RestController
public class SampleController {

  @Value("${akka.stream.alpakka.spring.web.actor-system-name}")
  private String actorSystemName;

  @Autowired private ActorSystem system;

  @RequestMapping("/")
  public Source<String, NotUsed> index() {
    return Source.repeat("Hello world!").intersperse("\n").take(10);
  }

  @PostConstruct
  public void setup() {
    LoggingAdapter log = system.log();
    log.info("Injected ActorSystem Name -> {}", system.name());
    log.info("Property ActorSystemName -> {}", actorSystemName);
    Assert.isTrue((system.name().equals(actorSystemName)), "Validating ActorSystem name");
  }
}
// #use
