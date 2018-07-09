/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference.javadsl;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.reference.SourceSettings;
import akka.stream.alpakka.reference.ReferenceWriteMessage;
import akka.stream.alpakka.reference.ReferenceReadMessage;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;

import scala.concurrent.ExecutionContext;
import scala.compat.java8.FutureConverters;

public class Reference {

  public static Source<ReferenceReadMessage, CompletionStage<Done>> source(
      SourceSettings settings) {
    return akka.stream.alpakka.reference.scaladsl.Reference.source(settings)
        .mapMaterializedValue(FutureConverters::toJava)
        .asJava();
  }

  /**
   * Only convert the flow type, as the materialized value type is the same between Java and Scala.
   */
  public static Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flow() {
    return akka.stream.alpakka.reference.scaladsl.Reference.flow().asJava();
  }

  /** In Java API take Executor as parameter if the operator needs to perform asynchronous tasks. */
  public static Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flowAsyncMapped(
      Executor ex) {
    return akka.stream.alpakka.reference.scaladsl.Reference.flowAsyncMapped(
            ExecutionContext.fromExecutor(ex))
        .asJava();
  }
}
