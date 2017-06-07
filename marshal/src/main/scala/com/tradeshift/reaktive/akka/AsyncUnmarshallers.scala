package com.tradeshift.reaktive.akka

import akka.japi.function.Function3
import scala.concurrent.ExecutionContext
import akka.stream.Materializer
import akka.http.javadsl.unmarshalling.Unmarshaller
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters._
import akka.http.javadsl.model.HttpEntity
import akka.stream.javadsl.Source
import akka.util.ByteString
import akka.NotUsed

object AsyncUnmarshallers {
  // FIXME find out of these are still needed, otherwise create a PR into akka-http itself.
  
  def withMaterializer[A, B](f: Function3[ExecutionContext, Materializer, A, CompletionStage[B]]): Unmarshaller[A, B] =
    akka.http.scaladsl.unmarshalling.Unmarshaller.withMaterializer(ctx => mat => a => f.apply(ctx, mat, a).toScala)
    
  def entityToStream(): Unmarshaller[HttpEntity, Source[ByteString, NotUsed]] = 
    Unmarshaller.sync(new java.util.function.Function[HttpEntity, Source[ByteString, NotUsed]] {
      override def apply(entity: HttpEntity) = entity.getDataBytes().asScala.mapMaterializedValue(obj => NotUsed).asJava
    })
}