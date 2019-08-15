/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.dynamodb.AwsOp;
import akka.stream.alpakka.dynamodb.javadsl.DynamoDb;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ExampleTest {

  static ActorSystem system;
  static Materializer materializer;
  static DynamoDbAsyncClient client;

  @BeforeClass
  public static void setup() throws Exception {
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);
    final DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .region(Region.AWS_GLOBAL)
            .endpointOverride(new URI("http://localhost:8001/"))
            .build();

    ExampleTest.system = system;
    ExampleTest.materializer = materializer;
    ExampleTest.client = client;
  }

  @AfterClass
  public static void tearDown() {
    client.close();
    system.terminate();
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void listTables() throws Exception {
    // #simple-request
    final CompletionStage<ListTablesResponse> listTables =
        DynamoDb.single(client, AwsOp.ListTables(), ListTablesRequest.builder().build(), materializer);
    // #simple-request
    ListTablesResponse result = listTables.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertNotNull(result.tableNames());
  }

  @Test
  public void allowMultipleRequests() throws Exception {
    // #flow
    Source<String, NotUsed> tableArnSource =
        Source.single(CreateTableRequest.builder().tableName("testTable").build())
            .via(DynamoDb.flow(client, AwsOp.CreateTable(), 1))
            .map(result -> result.tableDescription().tableArn());
    // #flow

    CompletionStage<List<String>> streamCompletion =
        tableArnSource.runWith(Sink.seq(), materializer);
    try {
      List<String> strings = streamCompletion.toCompletableFuture().get(5, TimeUnit.SECONDS);
      fail("expected missing schema");
    } catch (ExecutionException expected) {
      // expected
    }
  }

  @Test
  public void paginated() throws Exception {
    // #paginated
    Source<ScanResponse, NotUsed> scanPages =
        DynamoDb.source(client, AwsOp.Scan(), ScanRequest.builder().tableName("testTable").build());
    // #paginated
    CompletionStage<List<ScanResponse>> streamCompletion =
        scanPages.runWith(Sink.seq(), materializer);
    try {
      List<ScanResponse> strings = streamCompletion.toCompletableFuture().get(1, TimeUnit.SECONDS);
      fail("expected missing schema");
    } catch (ExecutionException expected) {
      // expected
    }
  }
}
