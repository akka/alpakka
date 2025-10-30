/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.sqs.javadsl;

import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.testkit.javadsl.TestKit;
// #init-client
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

// #init-client
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class BaseSqsTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  protected static ActorSystem system;
  private static SqsAsyncClient sqsClientForClose;

  private boolean initialized = false;
  protected String sqsEndpoint = "http://localhost:9324";
  protected SqsAsyncClient sqsClient;

  @BeforeClass
  public static void setup() {
    // #init-mat
    system = ActorSystem.create();
    // #init-mat
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (sqsClientForClose != null) {
      sqsClientForClose.close();
    }
    Http.get(system)
        .shutdownAllConnectionPools()
        .thenRun(() -> TestKit.shutdownActorSystem(system))
        .toCompletableFuture()
        .get(2, TimeUnit.SECONDS);
  }

  @Before
  public void setupBefore() {
    if (!initialized) {
      sqsClient = createAsyncClient(sqsEndpoint);
      sqsClientForClose = sqsClient;
      initialized = true;
    }
  }

  private SqsAsyncClient createAsyncClient(String sqsEndpoint) {
    // #init-client
    // Don't encode credentials in your source code!
    // see https://doc.akka.io/libraries/alpakka/current/aws-shared-configuration.html
    StaticCredentialsProvider credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"));
    SqsAsyncClient sqsClient =
        SqsAsyncClient.builder()
            .credentialsProvider(credentialsProvider)
            // #init-client
            .endpointOverride(URI.create(sqsEndpoint))
            // #init-client
            .region(Region.EU_CENTRAL_1)
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            // Possibility to configure the retry policy
            // see https://doc.akka.io/libraries/alpakka/current/aws-shared-configuration.html
            // .overrideConfiguration(...)
            .build();

    system.registerOnTermination(() -> sqsClient.close());
    // #init-client
    return sqsClient;
  }

  protected String randomQueueUrl() throws Exception {
    return sqsClient
        .createQueue(
            CreateQueueRequest.builder()
                .queueName(String.format("queue-%s", new Random().nextInt()))
                .build())
        .get(2, TimeUnit.SECONDS)
        .queueUrl();
  }
}
