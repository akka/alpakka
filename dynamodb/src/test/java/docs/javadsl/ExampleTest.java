/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.dynamodb.AwsOp;
import akka.stream.alpakka.dynamodb.DynamoAttributes;
import akka.stream.alpakka.dynamodb.DynamoClient;
import akka.stream.alpakka.dynamodb.DynamoSettings;
import akka.stream.alpakka.dynamodb.javadsl.DynamoDb;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.model.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ExampleTest {

  static ActorSystem system;
  static Materializer materializer;

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("aws.accessKeyId", "someKeyId");
    System.setProperty("aws.secretKey", "someSecretKey");

    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);

    ExampleTest.system = system;
    ExampleTest.materializer = materializer;
  }

  @AfterClass
  public static void tearDown() {
    system.terminate();
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void setCredentialsProvider() {
    // #credentials-provider
    AWSCredentialsProvider provider = // ...
        // #credentials-provider
        new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey"));
    // #credentials-provider
    DynamoSettings settings =
        DynamoSettings.create("eu-west-1", "localhost").withCredentialsProvider(provider);
    // #credentials-provider
    assertEquals("eu-west-1", settings.region());
  }

  @Test
  public void listTables() throws Exception {
    // #simple-request
    final Source<ListTablesResult, NotUsed> listTables =
        DynamoDb.listTables(new ListTablesRequest());
    // #simple-request
    ListTablesResult result =
        listTables
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertNotNull(result.getTableNames());
  }

  @Test
  public void allowMultipleRequests() throws Exception {
    // #flow
    Source<String, NotUsed> tableArnSource =
        Source.single(AwsOp.create(new CreateTableRequest().withTableName("testTable")))
            .via(DynamoDb.flow())
            .map(r -> (CreateTableResult) r)
            .map(result -> result.getTableDescription().getTableArn());
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
    Source<ScanResult, NotUsed> scanPages =
        DynamoDb.scanAll(new ScanRequest().withTableName("testTable"));
    // #paginated
    CompletionStage<List<ScanResult>> streamCompletion =
        scanPages.runWith(Sink.seq(), materializer);
    try {
      List<ScanResult> strings = streamCompletion.toCompletableFuture().get(1, TimeUnit.SECONDS);
      fail("expected missing schema");
    } catch (ExecutionException expected) {
      // expected
    }
  }

  @Test
  public void useClientFromAttributes() throws Exception {
    // #attributes
    final DynamoSettings settings = DynamoSettings.create(system).withRegion("custom-region");
    final DynamoClient client = DynamoClient.create(settings, system, materializer);

    final Source<ListTablesResult, NotUsed> source =
        DynamoDb.listTables(new ListTablesRequest())
            .withAttributes(DynamoAttributes.client(client));
    // #attributes

    ListTablesResult result =
        source.runWith(Sink.head(), materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertNotNull(result.getTableNames());
  }
}
