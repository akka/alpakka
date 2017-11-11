/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.spring.web;

//#configure

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

@Configuration
@ConditionalOnClass(akka.stream.javadsl.Source.class)
public class SpringWebAkkaStreamsConfiguration {

    private final ActorSystem system;
    private final ActorMaterializer mat;

    @Autowired
    public SpringWebAkkaStreamsConfiguration(RequestMappingHandlerAdapter requestMappingHandlerAdapter) {
        final ReactiveAdapterRegistry registry = requestMappingHandlerAdapter.getReactiveAdapterRegistry();

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

//#configure
