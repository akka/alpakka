/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.geode.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.geode.AkkaPdxSerializer;
import akka.stream.alpakka.geode.GeodeSettings;
import akka.stream.alpakka.geode.RegionSettings;
import akka.stream.alpakka.geode.impl.GeodeCache;

import akka.stream.alpakka.geode.impl.stage.GeodeFiniteSourceStage;
import akka.stream.alpakka.geode.impl.stage.GeodeFlowStage;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.geode.cache.client.ClientCacheFactory;

import scala.jdk.javaapi.FutureConverters;

import java.util.concurrent.CompletionStage;

/** Java API: Geode client without server event subscription. */
public class Geode extends GeodeCache {

  final GeodeSettings geodeSettings;

  public Geode(GeodeSettings settings) {
    super(settings);
    this.geodeSettings = settings;
  }

  @Override
  public ClientCacheFactory configure(ClientCacheFactory factory) {
    return factory.addPoolLocator(geodeSettings.hostname(), geodeSettings.port());
  }

  public <V> Source<V, CompletionStage<Done>> query(String query, AkkaPdxSerializer<V> serializer) {

    registerPDXSerializer(serializer, serializer.clazz());
    return Source.fromGraph(new GeodeFiniteSourceStage<V>(cache(), query))
        .mapMaterializedValue(FutureConverters::asJava);
  }

  public <K, V> Flow<V, V, NotUsed> flow(
      RegionSettings<K, V> regionSettings, AkkaPdxSerializer<V> serializer) {

    registerPDXSerializer(serializer, serializer.clazz());

    return Flow.fromGraph(new GeodeFlowStage<K, V>(cache(), regionSettings));
  }

  public <K, V> Sink<V, CompletionStage<Done>> sink(
      RegionSettings<K, V> regionSettings, AkkaPdxSerializer<V> serializer) {
    return flow(regionSettings, serializer).toMat(Sink.ignore(), Keep.right());
  }

  public void close() {
    close(false);
  }
}
