/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.geode.scaladsl

import akka.stream.alpakka.geode.impl._
import akka.stream.alpakka.geode.impl.pdx.{PdxDecoder, PdxEncoder, ShapelessPdxSerializer}
import akka.stream.alpakka.geode.impl.stage.{GeodeContinuousSourceStage, GeodeFiniteSourceStage, GeodeFlowStage}
import akka.stream.alpakka.geode.{AkkaPdxSerializer, GeodeSettings, RegionSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import org.apache.geode.cache.client.ClientCacheFactory

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Scala API: Geode client without server event subscription.
 */
class Geode(settings: GeodeSettings) extends GeodeCache(settings) {

  /**
   * This method is overloaded by [[PoolSubscription]] to provide server event subscriptions.
   */
  override protected def configure(factory: ClientCacheFactory): ClientCacheFactory =
    factory.addPoolLocator(settings.hostname, settings.port)

  def query[V <: AnyRef](query: String, serializer: AkkaPdxSerializer[V]): Source[V, Future[Done]] = {

    registerPDXSerializer(serializer, serializer.clazz)

    Source.fromGraph(new GeodeFiniteSourceStage[V](cache, query))
  }

  def flow[K, V <: AnyRef](settings: RegionSettings[K, V], serializer: AkkaPdxSerializer[V]): Flow[V, V, NotUsed] = {

    registerPDXSerializer(serializer, serializer.clazz)

    Flow.fromGraph(new GeodeFlowStage[K, V](cache, settings))
  }

  def sink[K, V <: AnyRef](settings: RegionSettings[K, V], serializer: AkkaPdxSerializer[V]): Sink[V, Future[Done]] =
    Flow[V].via(flow(settings, serializer)).toMat(Sink.ignore)(Keep.right)

  /**
   * Shapeless powered implicit serializer.
   */
  def query[V <: AnyRef](
      query: String
  )(implicit tag: ClassTag[V], enc: PdxEncoder[V], dec: PdxDecoder[V]): Source[V, Future[Done]] = {

    registerPDXSerializer(new ShapelessPdxSerializer[V](enc, dec), tag.runtimeClass)

    Source.fromGraph(new GeodeFiniteSourceStage[V](cache, query))
  }

  /**
   * Shapeless powered implicit serializer.
   */
  def flow[K, V <: AnyRef](
      settings: RegionSettings[K, V]
  )(implicit tag: ClassTag[V], enc: PdxEncoder[V], dec: PdxDecoder[V]): Flow[V, V, NotUsed] = {

    registerPDXSerializer(new ShapelessPdxSerializer[V](enc, dec), tag.runtimeClass)

    Flow.fromGraph(new GeodeFlowStage[K, V](cache, settings))
  }

  /**
   * Shapeless powered implicit serializer.
   */
  def sink[K, V <: AnyRef](
      settings: RegionSettings[K, V]
  )(implicit tag: ClassTag[V], enc: PdxEncoder[V], dec: PdxDecoder[V]): Sink[V, Future[Done]] =
    Flow[V].via(flow(settings)).toMat(Sink.ignore)(Keep.right)

}

trait PoolSubscription extends Geode {

  /**
   * Pool subscription is mandatory for continuous query.
   */
  final override protected def configure(factory: ClientCacheFactory) =
    super.configure(factory).setPoolSubscriptionEnabled(true)

  def continuousQuery[V <: AnyRef](queryName: Symbol,
                                   query: String,
                                   serializer: AkkaPdxSerializer[V]): Source[V, Future[Done]] = {

    registerPDXSerializer(serializer, serializer.clazz)

    Source.fromGraph(new GeodeContinuousSourceStage[V](cache, queryName.name, query))
  }

  /**
   * Shapeless powered implicit serializer.
   */
  def continuousQuery[V <: AnyRef](
      queryName: Symbol,
      query: String
  )(implicit tag: ClassTag[V], enc: PdxEncoder[V], dec: PdxDecoder[V]): Source[V, Future[Done]] = {

    registerPDXSerializer(new ShapelessPdxSerializer[V](enc, dec), tag.runtimeClass)

    Source.fromGraph(new GeodeContinuousSourceStage[V](cache, queryName.name, query))
  }

  def closeContinuousQuery(queryName: Symbol) =
    for {
      qs <- Option(cache.getQueryService())
      query <- Option(qs.getCq(queryName.name))
    } yield (query.close())

}
