/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
// #init-client
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;

// #init-client
import akka.stream.alpakka.dynamodb.DynamoDbOp;
import akka.stream.alpakka.dynamodb.javadsl.DynamoDb;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
// #init-client
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

// #init-client
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

    // #init-client
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);
    final DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .region(Region.AWS_GLOBAL)
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            // #init-client
            .endpointOverride(new URI("http://localhost:8001/"))
            // #init-client
            .build();

    system.registerOnTermination(() -> client.close());

    // #init-client

    ExampleTest.system = system;
    ExampleTest.materializer = materializer;
    ExampleTest.client = client;
  }

  @AfterClass
  public static void tearDown() {
    client.close();
    TestKit.shutdownActorSystem(system);
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void listTables() throws Exception {
    // format: off
    // #simple-request
    final CompletionStage<ListTablesResponse> listTables =
        DynamoDb.single(
            client, DynamoDbOp.listTables(), ListTablesRequest.builder().build(), materializer);
    // #simple-request
    // format: on
    ListTablesResponse result = listTables.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertNotNull(result.tableNames());
  }

  @Test
  public void allowMultipleRequests() throws Exception {
    // #flow
    Source<DescribeTableResponse, NotUsed> tableArnSource =
        Source.single(CreateTableRequest.builder().tableName("testTable").build())
            .via(DynamoDb.flow(client, DynamoDbOp.createTable(), 1))
            .map(
                result ->
                    DescribeTableRequest.builder()
                        .tableName(result.tableDescription().tableName())
                        .build())
            .via(DynamoDb.flow(client, DynamoDbOp.describeTable(), 1));
    // #flow

    CompletionStage<List<DescribeTableResponse>> streamCompletion =
        tableArnSource.runWith(Sink.seq(), materializer);
    try {
      List<DescribeTableResponse> responses =
          streamCompletion.toCompletableFuture().get(5, TimeUnit.SECONDS);
      fail("expected missing schema");
    } catch (ExecutionException expected) {
      // expected
    }
  }

  @Test
  public void paginated() throws Exception {
    // #paginated
    Source<ScanResponse, NotUsed> scanPages =
        DynamoDb.source(
            client, DynamoDbOp.scan(), ScanRequest.builder().tableName("testTable").build());
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
