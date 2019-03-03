/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.kinesis.ShardSettings;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class KinesisTest {
  private static ActorSystem system;
  private static ActorMaterializer materializer;
  private static ShardSettings settings;
  private static AmazonKinesisAsync amazonKinesisAsync;

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("aws.accessKeyId", "someKeyId");
    System.setProperty("aws.secretKey", "someSecretKey");

    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);

    settings = ShardSettings.create("my-stream", "shard-id");
    amazonKinesisAsync = mock(AmazonKinesisAsync.class);
  }

  @AfterClass
  public static void afterAll() {
    TestKit.shutdownActorSystem(system);
  }

  //  @Ignore("This test appears to trigger a deadlock, see
  // https://github.com/akka/alpakka/issues/390")
  @Test
  public void PullRecord() throws Exception {

    when(amazonKinesisAsync.describeStream(anyString()))
        .thenReturn(
            new DescribeStreamResult()
                .withStreamDescription(
                    new StreamDescription()
                        .withShards(new Shard().withShardId("id"))
                        .withHasMoreShards(false)));
    when(amazonKinesisAsync.getShardIteratorAsync(any(), any()))
        .thenAnswer(
            (Answer)
                invocation -> {
                  AsyncHandler<GetShardIteratorRequest, GetShardIteratorResult> args =
                      (AsyncHandler<GetShardIteratorRequest, GetShardIteratorResult>)
                          invocation.getArguments()[1];
                  args.onSuccess(new GetShardIteratorRequest(), new GetShardIteratorResult());
                  return CompletableFuture.completedFuture(new GetShardIteratorResult());
                });

    when(amazonKinesisAsync.getRecordsAsync(any(), any()))
        .thenAnswer(
            (Answer)
                invocation -> {
                  AsyncHandler<GetRecordsRequest, GetRecordsResult> args =
                      (AsyncHandler<GetRecordsRequest, GetRecordsResult>)
                          invocation.getArguments()[1];
                  args.onSuccess(
                      new GetRecordsRequest(),
                      new GetRecordsResult()
                          .withRecords(new Record().withSequenceNumber("1"))
                          .withNextShardIterator("iter"));
                  return CompletableFuture.completedFuture(new GetRecordsResult());
                });

    final Source<Record, NotUsed> source = KinesisSource.basic(settings, amazonKinesisAsync);
    final CompletionStage<Record> record = source.runWith(Sink.head(), materializer);

    assertEquals("1", record.toCompletableFuture().get(10, TimeUnit.SECONDS).getSequenceNumber());
  }
}
