/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.spring.web;

import java.util.Objects;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ReactiveAdapterRegistry;

// #configure

import akka.actor.ActorSystem;

@Configuration
@ConditionalOnClass(akka.stream.javadsl.Source.class)
@EnableConfigurationProperties(SpringWebAkkaStreamsProperties.class)
public class SpringWebAkkaStreamsConfiguration {

  private static final String DEFAULT_ACTORY_SYSTEM_NAME = "SpringWebAkkaStreamsSystem";

  private final ActorSystem system;
  private final SpringWebAkkaStreamsProperties properties;

  public SpringWebAkkaStreamsConfiguration(final SpringWebAkkaStreamsProperties properties) {
    this.properties = properties;
    final ReactiveAdapterRegistry registry = ReactiveAdapterRegistry.getSharedInstance();

    system = ActorSystem.create(getActorSystemName(properties));
    new AkkaStreamsRegistrar(system).registerAdapters(registry);
  }

  @Bean
  @ConditionalOnMissingBean(ActorSystem.class)
  public ActorSystem getActorSystem() {
    return system;
  }

  public SpringWebAkkaStreamsProperties getProperties() {
    return properties;
  }

  private String getActorSystemName(final SpringWebAkkaStreamsProperties properties) {
    Objects.requireNonNull(
        properties,
        String.format(
            "%s is not present in application context",
            SpringWebAkkaStreamsProperties.class.getSimpleName()));

    if (isBlank(properties.getActorSystemName())) {
      return DEFAULT_ACTORY_SYSTEM_NAME;
    }

    return properties.getActorSystemName();
  }

  private boolean isBlank(String str) {
    return (str == null || str.isEmpty());
  }
}

// #configure
