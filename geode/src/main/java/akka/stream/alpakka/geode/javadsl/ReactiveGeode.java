/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.geode.AkkaPdxSerializer;
import akka.stream.alpakka.geode.GeodeSettings;
import akka.stream.alpakka.geode.RegionSettings;
import akka.stream.alpakka.geode.internal.GeodeCache;
import akka.stream.alpakka.geode.internal.stage.GeodeContinuousSourceStage;
import akka.stream.alpakka.geode.internal.stage.GeodeFiniteSourceStage;
import akka.stream.alpakka.geode.internal.stage.GeodeFlowStage;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import scala.Symbol;
import scala.concurrent.Future;

import java.util.concurrent.CompletionStage;


/**
 * Reactive geode without server event subscription. Cannot build continuous source.
 */
public class ReactiveGeode extends GeodeCache {

    final GeodeSettings geodeSettings;

    public ReactiveGeode(GeodeSettings settings) {
        super(settings);
        this.geodeSettings = settings;
    }

    @Override
    public ClientCacheFactory configure(ClientCacheFactory factory) {
        return factory.addPoolLocator(geodeSettings.hostname(), geodeSettings.port());
    }

    public <V> Source<V, Future<Done>> query( String query, AkkaPdxSerializer<V> serializer) {

        registerPDXSerializer(serializer, serializer.clazz());
        return Source.fromGraph(new GeodeFiniteSourceStage<V>(cache(), query));
    }


    public <K, V> Flow<V, V, NotUsed> flow(RegionSettings<K, V> regionSettings, AkkaPdxSerializer<V> serializer) {

        registerPDXSerializer(serializer, serializer.clazz());

        return Flow.fromGraph(new GeodeFlowStage<K, V>(cache(), regionSettings));

    }

    public <K, V> Sink<V, CompletionStage<Done>> sink(RegionSettings<K, V> regionSettings, AkkaPdxSerializer<V> serializer) {
        return flow(regionSettings, serializer)
                .toMat(Sink.ignore(), Keep.right());
    }

    public void close() {
        close(false);
    }


}

/**
 * Reactive geode with server event subscription. Can build continuous source.
 */
class ReactiveGeodeWithPoolSubscription extends ReactiveGeode  {

    /**
     * Subscribes to server event.
     * @return ClientCacheFactory with server event subscription.
     */
    final public ClientCacheFactory configure(ClientCacheFactory factory) {
        return super.configure(factory).setPoolSubscriptionEnabled(true);
    }

    public ReactiveGeodeWithPoolSubscription(GeodeSettings settings) {
        super(settings);
    }

    public <V> Source<V, Future<Done>> continuousQuery(String queryName, String query, AkkaPdxSerializer<V> serializer) {

        registerPDXSerializer(serializer, serializer.clazz());
        return Source.fromGraph(new GeodeContinuousSourceStage<V>(cache(), Symbol.apply(queryName), query));
    }

    public boolean closeContinuousQuery(String name) throws CqException {

        QueryService qs = cache().getQueryService();
        CqQuery query = qs.getCq(name);
        if (query == null)
            return false;
        query.close();
        return true;

    }

}