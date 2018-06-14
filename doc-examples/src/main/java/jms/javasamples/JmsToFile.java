/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package jms.javasamples;

// #sample
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.KillSwitch;
import akka.stream.Materializer;
import akka.stream.alpakka.jms.JmsConsumerSettings;
import akka.stream.alpakka.jms.JmsProducerSettings;
import akka.stream.alpakka.jms.javadsl.JmsConsumer;
import akka.stream.alpakka.jms.javadsl.JmsProducer;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

// #sample

import playground.ActiveMqBroker;
import scala.concurrent.ExecutionContext;

import javax.jms.ConnectionFactory;
import java.util.Arrays;

public class JmsToFile {

  public static void main(String[] args) throws Exception {
    JmsToFile me = new JmsToFile();
    me.run();
  }

  private final ActorSystem system = ActorSystem.create();
  private final Materializer materializer = ActorMaterializer.create(system);
  private final ExecutionContext ec = system.dispatcher();

  private void enqueue(ConnectionFactory connectionFactory, String... msgs) {
    Sink<String, ?> jmsSink =
        JmsProducer.textSink(
            JmsProducerSettings.create(connectionFactory).withQueue("test")
        );
    Source.from(Arrays.asList(msgs)).runWith(jmsSink, materializer);
  }


  private void run() throws Exception {
    ActiveMqBroker activeMqBroker = new ActiveMqBroker();
    activeMqBroker.start();

    ConnectionFactory connectionFactory = activeMqBroker.createConnectionFactory();
    enqueue(connectionFactory, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
    // #sample

    Source<String, KillSwitch> jmsSource =        // (1)
        JmsConsumer.textSource(
            JmsConsumerSettings.create(connectionFactory)
                .withBufferSize(10)
                .withQueue("test")
        );

    Sink<ByteString, CompletionStage<IOResult>> fileSink =
        FileIO.toPath(Paths.get("target/out.txt")); // (2)

    Pair<KillSwitch, CompletionStage<IOResult>> pair =
        jmsSource                              //: String
            .map(ByteString::fromString)       //: ByteString    (3)
            .toMat(fileSink, Keep.both())
            .run(materializer);

    // #sample

    KillSwitch runningSource = pair.first();
    CompletionStage<IOResult> streamCompletion = pair.second();

    runningSource.shutdown();
    streamCompletion.thenAccept(res -> system.terminate());
    system.getWhenTerminated().thenAccept(t ->
        activeMqBroker.stop(ec)
    );

  }
}
