/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.spring.web;

import akka.actor.ActorSystem;
import akka.actor.ClassicActorSystemProvider;
import akka.stream.Materializer;
import akka.stream.javadsl.AsPublisher;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.util.Assert;

import static org.springframework.core.ReactiveTypeDescriptor.multiValue;

public class AkkaStreamsRegistrar {

  private final ActorSystem system;

  /**
   * deprecated, use {@link #AkkaStreamsRegistrar(ClassicActorSystemProvider)}.
   *
   * @deprecated pass in the actor system instead of the materializer, since 3.0.0
   */
  @Deprecated
  public AkkaStreamsRegistrar(Materializer materializer) {
    this(materializer.system());
  }

  public AkkaStreamsRegistrar(ClassicActorSystemProvider system) {
    this.system = system.classicSystem();
  }

  public void registerAdapters(ReactiveAdapterRegistry registry) {
    Assert.notNull(registry, "registry must not be null");
    registry.registerReactiveType(
        multiValue(akka.stream.javadsl.Source.class, akka.stream.javadsl.Source::empty),
        source ->
            ((akka.stream.javadsl.Source<?, ?>) source)
                .runWith(akka.stream.javadsl.Sink.asPublisher(AsPublisher.WITH_FANOUT), system),
        akka.stream.javadsl.Source::fromPublisher);

    registry.registerReactiveType(
        multiValue(akka.stream.scaladsl.Source.class, akka.stream.scaladsl.Source::empty),
        source ->
            ((akka.stream.scaladsl.Source<?, ?>) source)
                .runWith(
                    akka.stream.scaladsl.Sink.asPublisher(true),
                    Materializer.matFromSystem(system)),
        akka.stream.scaladsl.Source::fromPublisher);
  }
}
