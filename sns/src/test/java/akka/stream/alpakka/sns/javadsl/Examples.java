/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sns.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;

import java.util.concurrent.CompletionStage;

public class Examples {

  // #init-client
  BasicAWSCredentials credentials = new BasicAWSCredentials("x", "x");
  AmazonSNSAsync snsClient =
      AmazonSNSAsyncClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(credentials))
          .build();
  // #init-client

  // #init-system
  ActorSystem system = ActorSystem.create();
  ActorMaterializer materializer = ActorMaterializer.create(system);
  // #init-system

  // #use-sink
  CompletionStage<Done> stringSink =
      Source.single("message")
          .runWith(SnsPublisher.createSink("topic-arn", snsClient), materializer);

  CompletionStage<Done> publishRequestSink =
      Source.single(new PublishRequest().withMessage("message"))
          .runWith(SnsPublisher.createPublishSink("topic-arn", snsClient), materializer);
  // #use-sink

  // #use-flow
  CompletionStage<Done> stringFlow =
      Source.single("message")
          .via(SnsPublisher.createFlow("topic-arn", snsClient))
          .runWith(Sink.ignore(), materializer);

  CompletionStage<Done> publishRequestFlow =
      Source.single(new PublishRequest().withMessage("message"))
          .via(SnsPublisher.createPublishFlow("topic-arn", snsClient))
          .runWith(Sink.ignore(), materializer);
  // #use-flow

}
