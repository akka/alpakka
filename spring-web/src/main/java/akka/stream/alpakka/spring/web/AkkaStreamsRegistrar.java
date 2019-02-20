/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.spring.web;

import akka.stream.Materializer;
import akka.stream.javadsl.AsPublisher;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.util.Assert;

import static org.springframework.core.ReactiveTypeDescriptor.multiValue;

public class AkkaStreamsRegistrar {

  private final Materializer materializer;

  public AkkaStreamsRegistrar(Materializer mat) {
    materializer = mat;
  }

  public void registerAdapters(ReactiveAdapterRegistry registry) {
    Assert.notNull(registry, "registry must not be null");
    registry.registerReactiveType(
        multiValue(akka.stream.javadsl.Source.class, akka.stream.javadsl.Source::empty),
        source ->
            ((akka.stream.javadsl.Source<?, ?>) source)
                .runWith(
                    akka.stream.javadsl.Sink.asPublisher(AsPublisher.WITH_FANOUT), materializer),
        akka.stream.javadsl.Source::fromPublisher);

    registry.registerReactiveType(
        multiValue(akka.stream.scaladsl.Source.class, akka.stream.scaladsl.Source::empty),
        source ->
            ((akka.stream.scaladsl.Source<?, ?>) source)
                .runWith(akka.stream.scaladsl.Sink.asPublisher(true), materializer),
        akka.stream.scaladsl.Source::fromPublisher);
  }
}
