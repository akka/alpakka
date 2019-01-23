/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.spring.web;

// #configure

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ReactiveAdapterRegistry;

@Configuration
@ConditionalOnClass(akka.stream.javadsl.Source.class)
public class SpringWebAkkaStreamsConfiguration {

  private final ActorSystem system;
  private final ActorMaterializer mat;

  public SpringWebAkkaStreamsConfiguration() {
    final ReactiveAdapterRegistry registry = ReactiveAdapterRegistry.getSharedInstance();

    system = ActorSystem.create("SpringWebAkkaStreamsSystem");
    mat = ActorMaterializer.create(system);
    new AkkaStreamsRegistrar(mat).registerAdapters(registry);
    
  }

  @Bean
  @ConditionalOnMissingBean(ActorSystem.class)
  public ActorSystem getActorSystem() {
    return system;
  }

  @Bean
  @ConditionalOnMissingBean(Materializer.class)
  public ActorMaterializer getMaterializer() {
    return mat;
  }
}

// #configure
