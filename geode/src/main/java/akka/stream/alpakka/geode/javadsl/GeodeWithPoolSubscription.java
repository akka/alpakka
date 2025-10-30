/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.geode.javadsl;

import akka.Done;
import akka.stream.alpakka.geode.AkkaPdxSerializer;
import akka.stream.alpakka.geode.GeodeSettings;
import akka.stream.alpakka.geode.impl.stage.GeodeContinuousSourceStage;
import akka.stream.javadsl.Source;
import java.util.concurrent.CompletionStage;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import scala.jdk.javaapi.FutureConverters;

/** Java API: Geode client with server event subscription. Can build continuous sources. */
public class GeodeWithPoolSubscription extends Geode {

  /**
   * Subscribes to server events.
   *
   * @return ClientCacheFactory with server event subscription.
   */
  public final ClientCacheFactory configure(ClientCacheFactory factory) {
    return super.configure(factory).setPoolSubscriptionEnabled(true);
  }

  public GeodeWithPoolSubscription(GeodeSettings settings) {
    super(settings);
  }

  public <V> Source<V, CompletionStage<Done>> continuousQuery(
      String queryName, String query, AkkaPdxSerializer<V> serializer) {
    registerPDXSerializer(serializer, serializer.clazz());
    return Source.fromGraph(new GeodeContinuousSourceStage<V>(cache(), queryName, query))
        .mapMaterializedValue(FutureConverters::asJava);
  }

  public boolean closeContinuousQuery(String name) throws CqException {
    QueryService qs = cache().getQueryService();
    CqQuery query = qs.getCq(name);
    if (query == null) return false;
    query.close();
    return true;
  }
}
