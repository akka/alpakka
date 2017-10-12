/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.spring.web;

//#configure 
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.spring.web.AkkaStreamsRegistrar;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

@Configuration
public class MyAppConfiguration {

  private final ActorSystem system;
  private final ActorMaterializer mat;

  @Autowired
  public MyAppConfiguration(RequestMappingHandlerAdapter requestMappingHandlerAdapter) {
    final ReactiveAdapterRegistry registry = requestMappingHandlerAdapter.getReactiveAdapterRegistry();

    system = ActorSystem.create("System");
    mat = ActorMaterializer.create(system);
    new AkkaStreamsRegistrar(mat).registerAdapters(registry);
  }
  
  @Bean
  public ActorSystem getSystem() {
    return system;
  }

  @Bean
  public ActorMaterializer getMaterializer() {
    return mat;
  }
}
//#configure 

