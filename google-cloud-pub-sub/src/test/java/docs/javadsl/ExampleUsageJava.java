/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.japi.Pair;
import akka.stream.RestartSettings;
import akka.stream.alpakka.googlecloud.pubsub.*;
import akka.stream.alpakka.googlecloud.pubsub.javadsl.GooglePubSub;
import akka.stream.javadsl.*;
import com.google.common.collect.Lists;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class ExampleUsageJava {

  private static void example() throws NoSuchAlgorithmException, InvalidKeySpecException {

    // #init-system
    ActorSystem system = ActorSystem.create();
    PubSubConfig config = PubSubConfig.create();
    String topic = "topic1";
    String subscription = "subscription1";
    // #init-system

    // #publish-single
    PublishMessage publishMessage =
        PublishMessage.create(new String(Base64.getEncoder().encode("Hello Google!".getBytes())));
    PublishRequest publishRequest = PublishRequest.create(Lists.newArrayList(publishMessage));

    Source<PublishRequest, NotUsed> source = Source.single(publishRequest);

    Flow<PublishRequest, List<String>, NotUsed> publishFlow =
        GooglePubSub.publish(topic, config, 1);

    CompletionStage<List<List<String>>> publishedMessageIds =
        source.via(publishFlow).runWith(Sink.seq(), system);
    // #publish-single

    // #publish-single-with-context
    PublishMessage publishMessageWithContext =
        PublishMessage.create(new String(Base64.getEncoder().encode("Hello Google!".getBytes())));
    PublishRequest publishRequestWithContext =
        PublishRequest.create(Lists.newArrayList(publishMessageWithContext));
    String context = "publishRequestId";

    Source<Pair<PublishRequest, String>, NotUsed> sourceWithContext =
        Source.single(Pair.apply(publishRequestWithContext, context));

    FlowWithContext<PublishRequest, String, List<String>, String, NotUsed> publishFlowWithContext =
        GooglePubSub.publishWithContext(topic, config, 1);

    CompletionStage<List<Pair<List<String>, String>>> publishedMessageIdsWithContext =
        sourceWithContext.via(publishFlowWithContext).runWith(Sink.seq(), system);
    // #publish-single-with-context

    // #publish-fast
    Source<PublishMessage, NotUsed> messageSource = Source.single(publishMessage);
    messageSource
        .groupedWithin(1000, Duration.ofMinutes(1))
        .map(messages -> PublishRequest.create(messages))
        .via(publishFlow)
        .runWith(Sink.ignore(), system);
    // #publish-fast

    // #publish with ordering key
    PublishMessage publishMessageWithOrderingKey =
            PublishMessage.create(new String(Base64.getEncoder().encode("Hello Google!".getBytes())), new HashMap<>(), Optional.of("my-ordering-key"));
    PublishRequest publishRequestWithOrderingKey = PublishRequest.create(Lists.newArrayList(publishMessage));

    CompletionStage<List<List<String>>> publishedMessageWithOrderingKeyIds =
            source.via(publishFlow).runWith(Sink.seq(), system);
    // #publish with ordering key

    // #subscribe
    Source<ReceivedMessage, Cancellable> subscriptionSource =
        GooglePubSub.subscribe(subscription, config);

    Sink<AcknowledgeRequest, CompletionStage<Done>> ackSink =
        GooglePubSub.acknowledge(subscription, config);

    subscriptionSource
        .map(
            message -> {
              // do something fun
              return message.ackId();
            })
        .groupedWithin(1000, Duration.ofMinutes(1))
        .map(acks -> AcknowledgeRequest.create(acks))
        .to(ackSink);
    // #subscribe

    // #subscribe-source-control
    Source.tick(Duration.ofSeconds(0), Duration.ofSeconds(10), Done.getInstance())
        .via(
            RestartFlow.withBackoff(
                RestartSettings.create(Duration.ofSeconds(1), Duration.ofSeconds(30), 0.2),
                () -> GooglePubSub.subscribeFlow(subscription, config)))
        .map(
            message -> {
              // do something fun
              return message.ackId();
            })
        .groupedWithin(1000, Duration.ofMinutes(1))
        .map(acks -> AcknowledgeRequest.create(acks))
        .to(ackSink);
    // #subscribe-source-control

    Sink<ReceivedMessage, CompletionStage<Done>> yourProcessingSink = Sink.ignore();

    // #subscribe-auto-ack
    Sink<ReceivedMessage, CompletionStage<Done>> processSink = yourProcessingSink;

    Sink<ReceivedMessage, NotUsed> batchAckSink =
        Flow.of(ReceivedMessage.class)
            .map(t -> t.ackId())
            .groupedWithin(1000, Duration.ofMinutes(1))
            .map(ids -> AcknowledgeRequest.create(ids))
            .to(ackSink);

    subscriptionSource.alsoTo(batchAckSink).to(processSink);
    // #subscribe-auto-ack
  }
}
