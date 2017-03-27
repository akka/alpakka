/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl;

import akka.stream.alpakka.sqs.scaladsl.SqsUtils$;
import com.amazonaws.services.sqs.AmazonSQSAsync;
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
    private String sqsEndpoint;
    protected AmazonSQSAsync sqsClient;

    @Before
    public void setupBefore() {
        if (!initialized) {
            sqsServer = SQSRestServerBuilder.withDynamicPort().start();
            sqsAddress = sqsServer.waitUntilStarted().localAddress();
            sqsPort = sqsAddress.getPort();
            sqsEndpoint = "http://" + sqsAddress.getHostName() + ":" + sqsPort;
            sqsClient = SqsUtils$.MODULE$.createAsyncClient(sqsEndpoint);
            initialized = true;
        }
    }

    @After
    public void tearDownAfter() {
        if (!tornDown){
            sqsServer.stopAndWait();
            tornDown = true;
        }
    }


}
