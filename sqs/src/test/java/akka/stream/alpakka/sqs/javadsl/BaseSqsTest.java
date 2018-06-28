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
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetSocketAddress;
import java.util.Random;

public abstract class BaseSqsTest {

  protected static ActorSystem system;
  protected static Materializer materializer;

  private boolean initialized = false;
  private boolean tornDown = false;
  private SQSRestServer sqsServer;
  private InetSocketAddress sqsAddress;
  protected String sqsEndpoint;
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
      sqsServer = SQSRestServerBuilder.withActorSystem(system).withDynamicPort().start();
      sqsAddress = sqsServer.waitUntilStarted().localAddress();
      sqsEndpoint = "http://" + sqsAddress.getHostName() + ":" + sqsAddress.getPort();
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

  @After
  public void tearDownAfter() {
    if (!tornDown) {
      sqsServer.stopAndWait();
      tornDown = true;
    }
  }

  protected String randomQueueUrl() {
    return sqsClient.createQueue(String.format("queue-%s", new Random().nextInt())).getQueueUrl();
  }
}
