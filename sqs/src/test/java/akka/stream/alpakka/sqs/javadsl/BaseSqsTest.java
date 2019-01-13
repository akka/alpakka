/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
// #init-client
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;

// #init-client
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Random;

public abstract class BaseSqsTest {

  protected static ActorSystem system;
  protected static Materializer materializer;

  private boolean initialized = false;
  protected String sqsEndpoint = "http://localhost:9324";
  protected AmazonSQSAsync sqsClient;

  @BeforeClass
  public static void setup() {
    // #init-mat
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    // #init-mat
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  @Before
  public void setupBefore() {
    if (!initialized) {
      sqsClient = createAsyncClient(sqsEndpoint);
      initialized = true;
    }
  }

  private AmazonSQSAsync createAsyncClient(String sqsEndpoint) {
    // #init-client
    AWSCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"));
    AmazonSQSAsync awsSqsClient =
        AmazonSQSAsyncClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(sqsEndpoint, "eu-central-1"))
            .build();
    system.registerOnTermination(() -> awsSqsClient.shutdown());
    // #init-client
    return awsSqsClient;
  }

  protected String randomQueueUrl() {
    return sqsClient.createQueue(String.format("queue-%s", new Random().nextInt())).getQueueUrl();
  }
}
