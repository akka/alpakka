/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.spring.web;

import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.AsPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.util.Assert;

import static org.springframework.core.ReactiveTypeDescriptor.multiValue;

@SpringBootApplication
public class DemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(DemoApplication.class, args);
  }

}
