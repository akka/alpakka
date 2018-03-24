/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.googlecloud.pubsub.javadsl.GooglePubSubGrpc;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import scala.concurrent.duration.FiniteDuration;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class ExampleUsageJavaGrpc {

    private static void example() {

        //#init-mat
        ActorSystem system = ActorSystem.create();
        ActorMaterializer materializer = ActorMaterializer.create(system);
        //#init-mat

        //#init-credentials
        String projectId = "test-XXXXX";
        String topic = "topic1";
        String subscription = "subscription1";
        //#init-credentials

        //#publish-single
        PubsubMessage publishMessage =
                PubsubMessage.newBuilder().setMessageId("1").setData(ByteString.copyFromUtf8("Hello world!")).build();
        com.google.pubsub.v1.PublishRequest publishRequest =
                com.google.pubsub.v1.PublishRequest.newBuilder().addMessages(publishMessage).build();

        Source<com.google.pubsub.v1.PublishRequest, NotUsed> source = Source.single(publishRequest);

        PubSubConfig pubSubConfig = new PubSubConfig(
                "pubsub.googleapis.com",
                443,
                false,
                true,
                1000
        );

        GooglePubSubGrpc.GooglePubSubGrpcJava client =
                GooglePubSubGrpc.of(projectId, subscription, 1, pubSubConfig, system, materializer);

        Flow<com.google.pubsub.v1.PublishRequest, PublishResponse, NotUsed> publishFlow =
                client.publish();

        CompletionStage<List<PublishResponse>> publishedMessageIds =
                source.via(publishFlow).runWith(Sink.seq(), materializer);
        //#publish-single

        //#publish-fast
        Source<PubsubMessage, NotUsed> messageSource = Source.single(publishMessage);
        messageSource.groupedWithin(1000, FiniteDuration.apply(1, "min"))
                .map(messages -> com.google.pubsub.v1.PublishRequest
                        .newBuilder()
                        .setTopic(topic)
                        .addAllMessages(messages)
                        .build())
                .via(publishFlow)
                .runWith(Sink.ignore(), materializer);
        //#publish-fast


        //#subscribe
        Source<com.google.pubsub.v1.ReceivedMessage, NotUsed> subscriptionSource =
            client.subscribe();

        Sink<AcknowledgeRequest, CompletionStage<Done>> ackSink =
            client.acknowledge();

        // do something fun
        subscriptionSource.map(receivedMessage -> receivedMessage.getAckId())
                .groupedWithin(1000, FiniteDuration.apply(1, "min"))
                .map(acks -> AcknowledgeRequest.of(acks))
                .to(ackSink);
        //#subscribe


        Sink<com.google.pubsub.v1.ReceivedMessage, CompletionStage<Done>> yourProcessingSink = Sink.ignore();

        //#subscribe-auto-ack
        Sink<com.google.pubsub.v1.ReceivedMessage, CompletionStage<Done>> processSink = yourProcessingSink;

        Sink<com.google.pubsub.v1.ReceivedMessage, NotUsed> batchAckSink = Flow.of(com.google.pubsub.v1.ReceivedMessage.class)
                .map(t -> t.getAckId())
                .groupedWithin(1000, FiniteDuration.apply(1, "minute"))
                .map(ids -> AcknowledgeRequest.of(ids))
                .to(ackSink);

        subscriptionSource.alsoTo(batchAckSink).to(processSink);
        //#subscribe-auto-ack
    }

}
