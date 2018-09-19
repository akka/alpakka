/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.dynamodb.DynamoClient;
import akka.stream.alpakka.dynamodb.DynamoSettings;
import akka.stream.alpakka.dynamodb.javadsl.DynamoDbExternal;
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits.CreateTable;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.model.*;
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
  static DynamoSettings settings;
  static DynamoClient dynamoClient;

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("aws.accessKeyId", "someKeyId");
    System.setProperty("aws.secretKey", "someSecretKey");

    // #client-construct
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);

    final DynamoSettings settings = DynamoSettings.create(system);
    final DynamoClient dynamoClient = DynamoClient.create(settings, system, materializer);
    // #client-construct

    ExampleTest.system = system;
    ExampleTest.materializer = materializer;
    ExampleTest.settings = settings;
    ExampleTest.dynamoClient = dynamoClient;
  }

  @AfterClass
  public static void tearDown() {
    system.terminate();
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
    final CompletionStage<ListTablesResult> listTablesResultFuture =
        DynamoDbExternal.listTables(new ListTablesRequest(), dynamoClient);
    // #simple-request
    ListTablesResult result = listTablesResultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertNotNull(result.getTableNames());
  }

  @Test
  public void flow() throws Exception {
    // #flow
    Source<String, NotUsed> tableArnSource =
        Source.single(new CreateTable(new CreateTableRequest().withTableName("testTable")))
            .via(DynamoDbExternal.flow(dynamoClient))
            .map(result -> (CreateTableResult) result)
            .map(result -> result.getTableDescription().getTableArn());
    // #flow
    //    final Duration duration = Duration.create(5, "seconds");
    CompletionStage<List<String>> streamCompletion =
        tableArnSource.runWith(Sink.seq(), materializer);
    try {
      List<String> strings = streamCompletion.toCompletableFuture().get(1, TimeUnit.SECONDS);
      fail("expeced missing schema");
    } catch (ExecutionException expected) {
      // expected
    }
  }

  @Test
  public void paginated() throws Exception {
    // #paginated
    Source<ScanResult, NotUsed> scanPages =
        DynamoDbExternal.scanAll(new ScanRequest().withTableName("testTable"), dynamoClient);
    // #paginated
    CompletionStage<List<ScanResult>> streamCompletion =
        scanPages.runWith(Sink.seq(), materializer);
    try {
      List<ScanResult> strings = streamCompletion.toCompletableFuture().get(1, TimeUnit.SECONDS);
      fail("expeced missing schema");
    } catch (ExecutionException expected) {
      // expected
    }
  }
}
