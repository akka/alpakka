/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;

public abstract class BaseSqsTest {

    private boolean initialized = false;
    private boolean tornDown = false;
    private SQSRestServer sqsServer;
    private InetSocketAddress sqsAddress;
    private int sqsPort = 0;
    protected String sqsEndpoint;
    protected AWSCredentialsProvider credentialsProvider =
            new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"));
    protected AmazonSQSAsync sqsClient;

    @Before
    public void setupBefore() {
        if (!initialized) {
            sqsServer = SQSRestServerBuilder.withDynamicPort().start();
            sqsAddress = sqsServer.waitUntilStarted().localAddress();
            sqsPort = sqsAddress.getPort();
            sqsEndpoint = "http://" + sqsAddress.getHostName() + ":" + sqsPort;
            sqsClient = createAsyncClient(sqsEndpoint, credentialsProvider);
            initialized = true;
        }
    }

    AmazonSQSAsync createAsyncClient(String sqsEndpoint, AWSCredentialsProvider credentialsProvider) {
        //#init-client
        AmazonSQSAsync client = AmazonSQSAsyncClientBuilder.standard()
          .withCredentials(credentialsProvider)
          .withEndpointConfiguration(
                  new AwsClientBuilder.EndpointConfiguration(sqsEndpoint, "eu-central-1"))
          .build();
        //#init-client
        return client;
    }


    @After
    public void tearDownAfter() {
        if (!tornDown){
            sqsServer.stopAndWait();
            tornDown = true;
        }
    }


}
