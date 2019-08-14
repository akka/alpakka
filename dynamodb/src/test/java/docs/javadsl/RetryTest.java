/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.JavaPartialFunction;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.dynamodb.AwsOp;
import akka.stream.alpakka.dynamodb.ItemSpecOps;
import akka.stream.alpakka.dynamodb.impl.javadsl.RetryFlow;
import akka.stream.alpakka.dynamodb.javadsl.DynamoDb;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.util.Try;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

import scala.compat.java8.FunctionConverters.package$;
import static scala.compat.java8.FunctionConverters.package$.MODULE$;

public class RetryTest extends ItemSpecOps {

  private static package$ functionConverters = MODULE$;

  static ActorSystem system;
  static Materializer materializer;

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("aws.accessKeyId", "someKeyId");
    System.setProperty("aws.secretKey", "someSecretKey");

    final ActorSystem sys = ActorSystem.create();
    final Materializer mat = ActorMaterializer.create(sys);

    system = sys;
    materializer = mat;
  }

  @AfterClass
  public static void tearDown() {
    system.terminate();
  }

  @After
  public void checkForStageLeaks() throws Exception {
    DynamoDb.single(AwsOp.create(deleteTableRequest()), materializer)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);

    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Override
  public String tableName() {
    return "RetryTest";
  }

  @Test
  public void retryRequest() throws Exception {
    DynamoDb.single(AwsOp.create(createTableRequest()), materializer)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    DynamoDb.single(AwsOp.create(batchWriteLargeItemRequest(1, 25)), materializer)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    DynamoDb.single(AwsOp.create(batchWriteLargeItemRequest(26, 50)), materializer)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);

    final JavaPartialFunction<
            Pair<Try<BatchGetItemResult>, NotUsed>,
            akka.japi.Option<Collection<Pair<AwsOp.BatchGetItem, NotUsed>>>>
        retryMatcher =
            new JavaPartialFunction<>() {
              public akka.japi.Option<Collection<Pair<AwsOp.BatchGetItem, NotUsed>>> apply(
                  Pair<Try<BatchGetItemResult>, NotUsed> in, boolean isCheck) {
                final Try<BatchGetItemResult> response = in.first();
                if (response.isSuccess()) {
                  final BatchGetItemResult result = response.get();
                  if (result.getUnprocessedKeys().size() > 0) {
                    return akka.japi.Option.some(
                        Collections.singleton(
                            Pair.create(
                                AwsOp.create(batchGetItemRequest(result.getUnprocessedKeys())),
                                NotUsed.getInstance())));
                  } else {
                    return akka.japi.Option.none();
                  }
                } else {
                  return akka.japi.Option.none();
                }
              }
            };

    Flow<
            akka.japi.Pair<AwsOp.BatchGetItem, NotUsed>,
            akka.japi.Pair<Try<BatchGetItemResult>, NotUsed>,
            NotUsed>
        retryFlow =
            RetryFlow.withBackoff(
                8,
                Duration.ofMillis(10),
                Duration.ofSeconds(5),
                0,
                DynamoDb.<AwsOp.BatchGetItem, NotUsed>tryFlow()
                    .map(
                        pair ->
                            Pair.create(
                                pair.first()
                                    .map(
                                        functionConverters.asScalaFromFunction(
                                            r -> (BatchGetItemResult) r)),
                                pair.second())),
                retryMatcher);

    final long responses =
        Source.single(
                Pair.create(AwsOp.create(batchGetLargeItemRequest(1, 50)), NotUsed.getInstance()))
            .via(retryFlow)
            .runFold(0, (cnt, i) -> cnt + 1, materializer)
            .toCompletableFuture()
            .get(30, TimeUnit.SECONDS);

    assertEquals(2, responses);
  }
}
