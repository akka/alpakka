/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.amqp._

import scala.compat.java8.FutureConverters._

object AmqpConfirmFlow {

  /**
   * Java API: Creates an [[AmqpConfirmFlow]] that accepts [[akka.stream.alpakka.amqp.WriteMessage WriteMessage]] elements
   * and sends  [[akka.stream.alpakka.amqp.ConfirmMessage ConfirmMessage]] to the out
   *
   * This stage materializes to a CompletionStage<Done>, which can be used to know when the Sink completes, either normally
   * or because of an amqp failure
   */
  def create(
      settings: AmqpWriteSettings
  ): akka.stream.javadsl.Flow[WriteMessage, ConfirmMessage, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpConfirmFlow(settings).mapMaterializedValue(_.toJava).asJava

}
